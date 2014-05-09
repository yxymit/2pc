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
//import "bytes"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "bytes"
//import "bufio"
//import "io/ioutil"

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
  
  Type string  // Lock, Prep, Commit
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
  servers []string
  gid int64 // my replica group ID

  // Your definitions here.
  config shardmaster.Config
  //lastOp map[int64]LastOp
  exeseq int    // the next seq to be executed.
  db map[int]map[string]*Row //string // db[shardID][key] -> value

  // Transcation database here.
  // For centralized locking: per_row_lock = false
  dblock bool
  txn_id int
 
  // If a transaction is in the curr_txns, it must have been successfully locked.
  curr_txns map[int][]ReqArgs // curr_txns[txnid] -> requests
  txn_phase map[int]string // db[txn_id] -> "Locked"/"Prepared"/"Commited"
  lastReply map[int]LastReply // db[txn_id] -> Replies

  // For DEBUG
  failpoint string  // BeforePrepare, AfterPrepare, BeforeCommit, AfterCommit 

  // Parameters 
  persistent bool
  per_row_lock bool
}

func (kv *ShardKV) detectDup(op Op) (bool, interface{}) {
  _, ok := kv.txn_phase[op.Txn_id]

  if !ok {
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
    if theop.Type == Prep {
      _, ok := kv.curr_txns[theop.Txn_id]
      if ok { // theop.Txn_id has already locked the group/rows
        theop = kv.getPrepOp(theop.Txn_id)
//  DPrintfCLR(5, "[exeOp] txnid=%d, theop=%+v", theop.Txn_id, theop) 
      }
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
        
//  DPrintfCLR(4, "[Prepare_handler] get prepare operation do_op=%+v", do_op) 
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
    if do_op.Txn_id == theop.Txn_id && do_op.Type == theop.Type {
      return OK
    }  
    if theop.Type == Lock && do_op.Type == Lock && do_op.Txn_id != theop.Txn_id {
      if !kv.canLock(theop.Txn_id, theop.Txn) {
        return ErrNoLock
      }
    }
  }
}

func (kv *ShardKV) doLock(op Op) {
  if kv.dblock {
    log.Fatal("[doLock] Shit! Already locked")
  }
  if !kv.per_row_lock {
    kv.dblock = true
    kv.txn_id = op.Txn_id
  } else {
    for _, req := range op.Txn {
      shard_id := key2shard(req.Key)
      row, ok := kv.db[shard_id][req.Key]
      if ok {
        row.lock(req.Type, op.Txn_id)
      }
    }
  }
  kv.curr_txns[op.Txn_id] = op.Txn
  kv.txn_phase[op.Txn_id] = Lock
}

func (kv *ShardKV) doPrep(op Op) {
  _, ok := kv.curr_txns[op.Txn_id]
  if !ok {
    log.Fatal("[doPrep] The txnid has not locked")
  }
  
  kv.txn_phase[op.Txn_id] = Prep
  kv.lastReply[op.Txn_id] = LastReply{Prepare_ok: op.Prepare_ok, Reply_list: op.Reply_list}
//  DPrintfCLR(4, "[doPrep] txn_id = %d. lastReply=%+v", op.Txn_id, kv.lastReply) 
}


func (kv *ShardKV) readDisk(key string) *Row {
  filename := dirname(kv.gid,kv.me)+key
  var row *Row  
  if _, err := os.Stat(filename); err == nil {
    f, err := os.Open(filename)
    if err != nil {
      log.Fatal(err)
    }
    row = new(Row)
    row.init()
    dec:= gob.NewDecoder(f)
    err = dec.Decode(row)
    f.Close()
  } else {
    log.Fatalf("[Server] ReadDisk Fail: Filename = %s doesn't exit\n", filename)
  }

  return row
}

func (kv *ShardKV) writeDisk(key string, row *Row) {
  filename := dirname(kv.gid,kv.me)+key
  f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }
  buf := new(bytes.Buffer)
  enc := gob.NewEncoder(buf)
  err = enc.Encode(row)
  if err != nil {
    log.Fatal(err)
  }
  _, e := f.Write(buf.Bytes())
  if e != nil {
    log.Fatal(err)
  }
  f.Close()
}

func (kv *ShardKV) doCommit(op Op) {
//  if !kv.dblock || kv.txn_id != op.Txn_id {
//    log.Fatal("[doCommit] Shit! Not locked by me")
//  }
  _, ok := kv.curr_txns[op.Txn_id]
  if !ok {
    log.Fatal("[doCommit] The txnid has not locked")
  }

  if !op.Commit {
    // txn abort
    kv.txn_phase[op.Txn_id] = Commit
  } else {
    reply_list := kv.lastReply[op.Txn_id].Reply_list
    for _, theop := range reply_list {
      key := theop.Key
      val := theop.Value
      shard_id := key2shard(key)

      _, ok := kv.db[shard_id][key]
      if !ok {
        kv.db[shard_id][key] = new(Row)
        kv.db[shard_id][key].init()
        if kv.per_row_lock {
          kv.db[shard_id][key].lock(theop.Type, op.Txn_id)
        }
      }

      switch theop.Type {
      default:
        log.Fatalf("Operation %T not supported by the database\n", theop)
      case "Put":
        kv.db[shard_id][key].Value = val
        //comit to disk
        if kv.persistent {
          kv.writeDisk(key, kv.db[shard_id][key])
        }
      case "Get":
        //do nothing
      case "Add":
        kv.db[shard_id][key].Value = val
        //comit to disk
        if kv.persistent {
          kv.writeDisk(key, kv.db[shard_id][key])
        }
      }  
    }
    
    kv.txn_phase[op.Txn_id] = Commit
  }   
  // Release the locks
  if !kv.per_row_lock { 
    kv.dblock = false
  } else {
    for _, req := range kv.curr_txns[op.Txn_id] {
      shard_id := key2shard(req.Key)
      kv.db[shard_id][req.Key].release(req.Type, op.Txn_id)
    }
  }
}

func (kv *ShardKV) canLock(txnid int, reqs []ReqArgs) bool {
  if !kv.per_row_lock {
    if kv.dblock && kv.txn_id != txnid {
      return false
    } 
  } else {
    for _, req := range reqs {
      shard_id := key2shard(req.Key)
      row, ok := kv.db[shard_id][req.Key]
      if ok && row.conflict(req.Type) {
        return false
      }
    }
  }
  return true
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

  if !kv.canLock(args.Txn_id, args.Txn) {
    reply.Err = ErrNoLock
    return nil
  } else {
    reply.Err = kv.exeOp(myop)
    return nil
  }
}

func (kv *ShardKV) getPrepOp(txnid int) Op {
  prepare_ok := true
  reply_list := make([]ReqReply, 0)

  for _, theop := range kv.curr_txns[txnid] {
    key := theop.Key
    new_val := theop.Value

    shard_id := key2shard(key)

    if kv.persistent {
      _, ok0 := kv.db[shard_id]
      _, ok1 := kv.db[shard_id][key]
		
      if !ok0 || !ok1 {
        filename := dirname(kv.gid,kv.me)+key
        if _, err := os.Stat(filename); err == nil {
          value := kv.readDisk(key)
          kv.db[shard_id][key] = value
        }
      }
    }

    switch theop.Type {
    default:
      log.Fatalf("Operation %T not supported by the database\n", theop)

    case "Put":
      
      _, err := strconv.Atoi(new_val)
      if err != nil {
        log.Fatalf("Values are not integers\n")
      }
      reply_list = append(reply_list, ReqReply{"Put", key, new_val})
      
    case "Get":
      _, ok := kv.db[shard_id][key]
      var curr_val string 
      if ok {
        curr_val = kv.db[shard_id][key].Value
      } else {
        curr_val = "0"
      }
      reply_list = append(reply_list, ReqReply{"Get", key, curr_val})
      
    case "Add":
      
      curr_val := kv.db[shard_id][key].Value
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
    myop = Op{Type: Prep, Txn_id: txnid, Prepare_ok: prepare_ok, Reply_list: reply_list}
  } else {
    myop = Op{Type: Prep, Txn_id: txnid, Prepare_ok: prepare_ok}
  }
  return myop
}

func (kv *ShardKV) Prepare_handler(args *PrepArgs, reply *PrepReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  if kv.failpoint == "BeforePrepare" {
    kv.kill()
    return nil
  }

  myop := Op{Type:Prep, Txn_id:args.Txn_id}
//  DPrintfCLR(5, "[Prepare_handler] txnid=%d, myop=%+v", myop.Txn_id, myop) 
  dup, val := kv.detectDup(myop)
  if dup {
    reply.Err = val.(PrepReply).Err
    reply.Prepare_ok = val.(PrepReply).Prepare_ok
    reply.Replies = val.(PrepReply).Replies
    return nil
  }

  reply.Err = kv.exeOp(myop)
  
  if kv.failpoint == "AfterPrepare" {
    kv.kill()
    return nil
  }

  reply.Prepare_ok = kv.lastReply[args.Txn_id].Prepare_ok
  reply.Replies = kv.lastReply[args.Txn_id].Reply_list

//  DPrintfCLR(4, "[Prepare_handler] txn_id = %d. reply=%+v", args.Txn_id, reply) 
  return nil
}

func (kv *ShardKV) Commit_handler(args *CommitArgs, reply *CommitReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  if kv.failpoint == "BeforeCommit" {
    kv.kill()
    return nil
  }

  myop := Op{Type: Commit, Txn_id: args.Txn_id, Commit: args.Commit}
  
  dup, val := kv.detectDup(myop)
  if dup {
    reply.Err = val.(CommitReply).Err
    return nil
  }
  
  reply.Err = kv.exeOp(myop)
  if kv.failpoint == "AfterCommit" {
    kv.kill()
    return nil
  }
 
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

func (kv *ShardKV) Reboot() {
  if !kv.dead {
    log.Fatal("The server was not dead when reboot.")
  }
  kv.dead = false
  kv.db = make(map[int]map[string]*Row)
  for i := 0; i < NShards; i++ {
    kv.db[i] = make(map[string]*Row)
  }
  
  kv.txn_phase = make(map[int]string)
  kv.lastReply = make(map[int]LastReply)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px.Reboot()

  os.Remove(kv.servers[kv.me])
  l, e := net.Listen("unix", kv.servers[kv.me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

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
func StartServer(gid int64, servers []string, me int, persistent bool, failpoint string) *ShardKV {
  gob.Register(Op{}) 
  gob.Register(ReqArgs{}) 
  gob.Register(TxnArgs{}) 
  gob.Register(make(map[int]map[string]string))
  kv := new(ShardKV)
  kv.servers = servers 
  kv.me = me
  kv.gid = gid

  kv.per_row_lock = false
  kv.dblock = false


  // Your initialization code here.
  // Don't call Join().
  kv.db = make(map[int]map[string]*Row)
  for i := 0; i < shardmaster.NShards; i++ {
    kv.db[i] = make(map[string]*Row)
  }

  kv.persistent = persistent
  
  if kv.persistent {
    os.MkdirAll(dirname(gid,me),os.ModeDir)
  }
	
  //  curr_txn []ReqArgs

  kv.txn_phase = make(map[int]string)
  kv.lastReply = make(map[int]LastReply)
  kv.curr_txns = make(map[int][]ReqArgs)


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.MakePaxos(servers, me, rpcs, "gid_"+strconv.Itoa(int(gid))+"_server_"+strconv.Itoa(me), kv.persistent)


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

