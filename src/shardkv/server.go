package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "container/list"

const Debug=0
const CLR_0 = "\x1b[30;1m"
const CLR_R = "\x1b[31;1m"
const CLR_G = "\x1b[32;1m"
const CLR_Y = "\x1b[33;1m"
const CLR_B = "\x1b[34;1m"
const CLR_M = "\x1b[35;1m"
const CLR_C = "\x1b[36;1m"
const CLR_W = "\x1b[37;1m"
const CLR_N = "\x1b[0m"

func DPrintfCLR(src int, format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    CLR := "\x1b[3"+strconv.Itoa(src)+";1m"
    fmt.Printf(CLR+format+CLR_N+"\n", a...)
  }
  return
}


func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  
  Type string // 'Lock', 'Prep', 'Commit'
  Txn *list.List
  Txn_id int
  Op_id string

  // for Type == prep
  Prepare_ok bool
  Reply_list *list.List
  
  // for Type == commit
  Commit bool

/*
  // for Type == "Reconfig" only
  Preconfig shardmaster.Config
  Afterconfig shardmaster.Config

  Db map[int]map[string]string
  LastOp map[int64]LastOp
*/
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  config shardmaster.Config
  //lastOp map[int64]LastOp
  exeseq int    // the next seq to be executed.
  db map[int]map[string]string // db[shardID][key] -> value

  // Transcation database here.
  dblock bool
  txn_id int
  curr_txn *list.List
  //reply_list list.List

  txn_phase map[int]string // db[txn_id] -> "Locked"/"Prepared"/"Commited"
  lastReply map[int]LastReply // db[txn_id] -> Replies
}

func (kv *ShardKV) detectDup(op Op) bool {
  _, ok := kv.txn_phase[op.Txn_id]

  if !ok {
    return false
  }

  switch op.Type {
  case "Lock":
    return true
  case "Prep":
    if kv.txn_phase[op.Txn_id] == "Prepared" || kv.txn_phase[op.Txn_id] == "Commited" {
      return true
    } else {
      return false
    }
  case "Commited":
    if kv.txn_phase[op.Txn_id] == "Comitted" {
      return true
    } else {
      return false
    }
  default :
    log.Fatal("Unsupportted Operation Type")
  }
  return false
}


func (kv *ShardKV) insertPaxos(theop Op) Err {
  for {
    if kv.dblock && kv.txn_id != theop.Txn_id {
      return ErrNoLock
    }

    if kv.detectDup(theop) {
      return OK
    }

    kv.px.Start(kv.exeseq, theop)
    to := 10 * time.Millisecond
    for {
      decided, decop := kv.px.Status(kv.exeseq)
      if decided {
        do_op := decop.(Op)
        kv.exeseq++
        
        switch do_op.Type {
        case "Lock":
          kv.doLock(do_op)
        case "Prep":
          kv.doPrep(do_op)
        case "Commit":
          kv.doCommit(do_op)
        default:
        }

        if do_op.Op_id == theop.Op_id {
          return OK
        } 
      } else {
        time.Sleep(to)
        if to < time.Second {
          to *= 2
        } 
      }
    }

  }
}

func (kv *ShardKV) doLock(op Op) bool {
  if kv.dblock && kv.txn_id == op.Txn_id {
    kv.dblock = true
    kv.txn_id = op.Txn_id
    kv.curr_txn = op.Txn
    
    kv.txn_phase[op.Txn_id] = "Locked"

    return true
  }

  return false
  
}

func (kv *ShardKV) doPrep(op Op) bool {  
  if kv.dblock && kv.txn_id == op.Txn_id {

    kv.txn_phase[op.Txn_id] = "Prepared"
    kv.lastReply[op.Txn_id] = LastReply{Prepare_ok: op.Prepare_ok, Reply_list: op.Reply_list}
    return true
  }
  
  return false
}

func (kv *ShardKV) doCommit(op Op) bool {
  if kv.dblock && kv.txn_id == op.Txn_id {
    
    reply_list := kv.lastReply[op.Txn_id].Reply_list
    for e := reply_list.Front(); e != nil; e = e.Next() {
      theop := e.Value.(ReqReply)
      switch theop.Type {
      default:
        log.Fatalf("Operation %T not supported by the database", theop)
      case "Put":
        key := theop.Key
        val := theop.Value
        shard_id := key2shard(key)
        kv.db[shard_id][key] = val
      case "Get":
        //do nothing
      case "Add":
        key := theop.Key
        val := theop.Value
        shard_id := key2shard(key)
        kv.db[shard_id][key] = val
      }  
    }
    
    // TODO:: db has to be written into resistent storage
    kv.txn_phase[op.Txn_id] = "Commited"
    kv.dblock = false
    
    return true
  }
  
  return false
}


func (kv *ShardKV) Insert_txn(args *TxnArgs, reply *TxnReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  opid := strconv.Itoa(time.Now().Nanosecond())

  myop := Op{Type: "Lock", Txn: args.Txn, Txn_id: args.Txn_id, Op_id: opid}

  reply.Err = kv.insertPaxos(myop)

  return nil
  
}


func (kv *ShardKV) Prepare_handler(args *PrepArgs, reply *PrepReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  prepare_ok := true

  reply_list := list.New()

  for e := kv.curr_txn.Front(); e != nil; e = e.Next() {
    theop := e.Value.(ReqArgs)
    key := theop.Key
    new_val := theop.Value
    shard_id := key2shard(key)
    curr_val := kv.db[shard_id][key]

    switch theop.Type {
    default:
      log.Fatalf("Operation %T not supported by the database", theop)

    case "Put":
      
      _, err0 := strconv.Atoi(curr_val)
      _, err1 := strconv.Atoi(new_val)
      if err0 != nil || err1 != nil {
        log.Fatalf("Values are not integers\n")
      }

      reply_list.PushBack(ReqReply{Type:"Put", Key: key, Value: new_val})
      
    case "Get":
      
      reply_list.PushBack(ReqReply{Type:"Get", Key: theop.Key, Value: curr_val})
      
    case "Add":
      
      x, err0 := strconv.Atoi(curr_val)
      y, err1 := strconv.Atoi(new_val)
        
      if err0 != nil || err1 != nil {
        log.Fatalf("Values are not integers\n")
      }

      z := x + y

      if z < 0 {
        prepare_ok = false
        break
      } else {
        reply_list.PushBack(ReqReply{Type:"Add", Key: key, Value: strconv.Itoa(z)})
      }    
      
    }  
  }

  opid := strconv.Itoa(time.Now().Nanosecond())
  
  var myop Op
  
  if prepare_ok {
    myop = Op{Type: "Prep", Txn_id: args.Txn_id, Prepare_ok: prepare_ok, Reply_list: reply_list, Op_id: opid}
  } else {
    myop = Op{Type: "Prep", Txn_id: args.Txn_id, Prepare_ok: prepare_ok, Op_id: opid}
  }

  reply.Err = kv.insertPaxos(myop)

  reply.Prepare_ok = kv.lastReply[args.Txn_id].Prepare_ok
  reply.Replies = kv.lastReply[args.Txn_id].Reply_list

  return nil

}

func (kv *ShardKV) Commit_handler(args *CommitArgs, reply *CommitReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  opid := strconv.Itoa(time.Now().Nanosecond())

  myop := Op{Type: "Commit", Txn_id: args.Txn_id, Commit: args.Commit, Op_id: opid}

  reply.Err = kv.insertPaxos(myop)

  return nil

}

func (kv *ShardKV) poll(){
  kv.mu.Lock()
  defer kv.mu.Unlock()

  decided, decop := kv.px.Poll(kv.exeseq) 

   if decided {
     do_op := decop.(Op)
     kv.exeseq++
        
     switch do_op.Type {
     case "Lock":
       kv.doLock(do_op)
     case "Prep":
       kv.doPrep(do_op)
     case "Commit":
       kv.doCommit(do_op)
     default:
     }
   }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, servers []string, me int) *ShardKV {
  gob.Register(Op{}) 
  gob.Register(make(map[int]map[string]string))
    
  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid

  kv.dblock = false

  // Your initialization code here.
  // Don't call Join().
  kv.db = make(map[int]map[string]string)
  for i := 0; i < shardmaster.NShards; i++ {
    kv.db[i] = make(map[string]string)
  }

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      //kv.tick()
      kv.poll()
      time.Sleep(100 * time.Millisecond)
    }
  }()

  return kv
}

