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

const Debug=1
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
  
  Type string 
  Txn []ReqArgs
  Txn_id int
  
  // for Type == prep
  Prepare_ok bool
  Reply_list []ReqReply
  
  // for Type == commit
  Commit bool
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
  curr_txn []ReqArgs
//  curr_txn *list.List
  //reply_list list.List

  txn_phase map[int]string // db[txn_id] -> "Locked"/"Prepared"/"Commited"
  lastReply map[int]LastReply // db[txn_id] -> Replies
}

func (kv *ShardKV) detectDup(op Op) (bool, interface{}) {
  _, ok := kv.txn_phase[op.Txn_id]

  if !ok {
    if op.Type != Lock {
      log.Fatal("Not exist in txn_phase")
    }
    return false, nil
  }

  switch op.Type {
  case Lock:
    return true, TxnReply{"OK"}
  case Prep:
    if kv.txn_phase[op.Txn_id] == Prep {
      return true, PrepReply{"OK", kv.lastReply[op.Txn_id].Prepare_ok, kv.lastReply[op.Txn_id].Reply_list}
    } else if kv.txn_phase[op.Txn_id] == Commit {
      return true, PrepReply{Err:"OK"}
    } else {
      return false, nil
    }
  case Commit:
    if kv.txn_phase[op.Txn_id] == Commit {
      return true, CommitReply{"OK"}
    } else {
      return false, nil
    }
  default :
    log.Fatal("Unsuportted Operation Type")
  }
  return false, nil
}


func (kv *ShardKV) exeOp(theop Op) Err {
  for {
    if theop.Type == Prep && kv.txn_id == theop.Txn_id && kv.dblock {
      theop = kv.getPrepOp()
    }
    kv.px.Start(kv.exeseq, theop)
    to := 10 * time.Millisecond
    decided := false
    var decop interface{}
    var do_op Op
    for {
      decided, decop = kv.px.Status(kv.exeseq)
      if decided {
        do_op = decop.(Op)
        kv.exeseq++
        
        switch do_op.Type {
        case Lock:
          kv.doLock(do_op)
        case Prep:
          kv.doPrep(do_op)
        case Commit:
          kv.doCommit(do_op)
        default:
          log.Fatal("Unsuported op type")
        }
        break
      } else {
        time.Sleep(to)
        if to < time.Second {
          to *= 2
        } 
      }
    }
    if theop.Type == Lock && do_op.Txn_id != theop.Txn_id {
      return ErrNoLock
    } else if do_op.Txn_id == theop.Txn_id && do_op.Type == theop.Type {
      return OK
    }
  }
}

func (kv *ShardKV) doLock(op Op) {   
  if kv.dblock {
    log.Fatal("[doLock] Shit! Already locked")
  }
  kv.dblock = true
  kv.txn_id = op.Txn_id
  kv.curr_txn = op.Txn
  kv.txn_phase[op.Txn_id] = Lock
}

func (kv *ShardKV) doPrep(op Op) {  
  if !kv.dblock || kv.txn_id != op.Txn_id {
    log.Fatal("[doPrep] Shit! Not locked by me")
  }
  kv.txn_phase[op.Txn_id] = Prep
  kv.lastReply[op.Txn_id] = LastReply{Prepare_ok: op.Prepare_ok, Reply_list: op.Reply_list}
}

func (kv *ShardKV) doCommit(op Op) {
  if !kv.dblock || kv.txn_id != op.Txn_id {
    log.Fatal("[doPrep] Shit! Not locked by me")
  }
  
  if !op.Commit {
    return
  } else {
    reply_list := kv.lastReply[op.Txn_id].Reply_list
    for _, theop := range reply_list {
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
    kv.txn_phase[op.Txn_id] = Commit
    kv.dblock = false
  }
}


func (kv *ShardKV) Insert_txn(args *TxnArgs, reply *TxnReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  myop := Op{Type: Lock, Txn: args.Txn, Txn_id: args.Txn_id}
  
  dup, val := kv.detectDup(myop)
  if dup {
    reply.Err = val.(TxnReply).Err
    return nil
  }
  if kv.dblock && kv.txn_id != args.Txn_id {
    reply.Err = ErrNoLock
    return nil
  }

  reply.Err = kv.exeOp(myop)

  return nil
  
}

func (kv *ShardKV) getPrepOp() Op {
  prepare_ok := true
  reply_list := make([]ReqReply, 0)

  for _, theop := range kv.curr_txn {
    key := theop.Key
    new_val := theop.Value
    shard_id := key2shard(key)
    curr_val := kv.db[shard_id][key]

    switch theop.Type {
    default:
      log.Fatalf("Operation %T not supported by the database", theop)

    case "Put":
      
      _, err := strconv.Atoi(new_val)
      if err != nil {
        log.Fatalf("Values are not integers\n")
      }
      reply_list = append(reply_list, ReqReply{"Put", key, new_val})
      
    case "Get":
      
      reply_list = append(reply_list, ReqReply{"Get", key, new_val})
      
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
        reply_list = append(reply_list, ReqReply{"Add", key, strconv.Itoa(z)})
      }    
    }  
  }
  var myop Op
  if prepare_ok {
    myop = Op{Type: Prep, Txn_id: kv.txn_id, Prepare_ok: prepare_ok, Reply_list: reply_list}
  } else {
    myop = Op{Type: Prep, Txn_id: kv.txn_id, Prepare_ok: prepare_ok}
  }
  return myop
}

func (kv *ShardKV) Prepare_handler(args *PrepArgs, reply *PrepReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
 
  myop := Op{Type:Prep, Txn_id:args.Txn_id}
  dup, val := kv.detectDup(myop)
  if dup {
    reply.Err = val.(PrepReply).Err
    reply.Prepare_ok = val.(PrepReply).Prepare_ok
    reply.Replies = val.(PrepReply).Replies
    return nil
  }

  reply.Err = kv.exeOp(myop)

  reply.Prepare_ok = kv.lastReply[args.Txn_id].Prepare_ok
  reply.Replies = kv.lastReply[args.Txn_id].Reply_list

  return nil
}

func (kv *ShardKV) Commit_handler(args *CommitArgs, reply *CommitReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  myop := Op{Type: Commit, Txn_id: args.Txn_id, Commit: args.Commit}
  
  dup, val := kv.detectDup(myop)
  if dup {
    reply.Err = val.(CommitReply).Err
    return nil
  }
  
  reply.Err = kv.exeOp(myop)

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
     case Lock:
       kv.doLock(do_op)
     case Prep:
       kv.doPrep(do_op)
     case Commit:
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
  gob.Register(ReqArgs{}) 
  gob.Register(TxnArgs{}) 
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
//  curr_txn []ReqArgs

  kv.txn_phase = make(map[int]string)
  kv.lastReply = make(map[int]LastReply)



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

