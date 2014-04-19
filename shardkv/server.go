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
  Type string       // 'Get', 'Put', 'Reconfig'
  Key string
  Value string
  DoHash bool
  Cid int64         // the client issuing the request
  Rpcid int
  // for Type == "Reconfig" only
  Preconfig shardmaster.Config
  Afterconfig shardmaster.Config
  Db map[int]map[string]string
  LastOp map[int64]LastOp
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  config shardmaster.Config
  lastOp map[int64]LastOp
  exeseq int    // the next seq to be executed.
  db map[int]map[string]string // db[shardID][key] -> value
}

func (kv *ShardKV) ValidReq(clientid int64, rpcid int, key string) Err {
  if kv.config.Shards[key2shard(key)] != kv.gid {
    return ErrWrongGroup 
  }
  lastOp, ok := kv.lastOp[clientid]
  if !ok {
    return OK
  } else if rpcid == lastOp.Rpcid {
    return ErrDupReq
  } else if rpcid < lastOp.Rpcid {
    return ErrStaleReq
  } 

  return OK
}

func (kv *ShardKV) Copy(args *CopyArgs, reply *CopyReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  reply.Shardid = args.Shardid
  reply.LastOp = kv.lastOp
  for _, lop := range kv.lastOp {
    if lop.Err == "" {
      log.Fatal("Copy. Err")
    }
  }
  if args.ConfigNum > kv.config.Num {
    reply.Err = "Config not upto date"
  } else {
    reply.Shard = kv.db[args.Shardid]
    reply.Err = OK
  }

  return nil
}

// Will copy shards from remote group to local group
func (kv *ShardKV) copydb(beforeconfig, afterconfig shardmaster.Config) (map[int]map[string]string, map[int64]LastOp) {
  if beforeconfig.Num == 0 {
    return nil, nil
  }
  // need to unlock since two groups may request shards from each other at 
  // the same time, causing deadlocks
  kv.mu.Unlock()
  
  repchan := make(chan CopyReply)
  repcnt := 0
  for sid := 0; sid < shardmaster.NShards; sid ++ {
    if beforeconfig.Shards[sid] != kv.gid && afterconfig.Shards[sid] == kv.gid {
      repcnt ++
      // parallelize remote db copying. 
      go func(sid int) {
        gid := beforeconfig.Shards[sid]
        servers := beforeconfig.Groups[gid]
        for {
          for _, srv := range servers {
            args := CopyArgs{afterconfig.Num, sid}
            var reply CopyReply
            ok := call(srv, "ShardKV.Copy", &args, &reply)
            if ok && reply.Err == OK {
              repchan <- reply
              return 
            } 
          }
          time.Sleep(100 * time.Millisecond)
        }
      }(sid)
    }
  }
  
  mdb := make(map[int]map[string]string)
  lastOp := make(map[int64]LastOp)
  waitcnt := 0
  if repcnt != 0 {
    reccnt := 0
    done := false
    for !done {
      select {
        case reply := <-repchan :
          reccnt ++ 
//          mdb[reply.Shardid] = make(map[string]string)
          mdb[reply.Shardid] = reply.Shard
          for cid, lop := range reply.LastOp {
            _, ok := lastOp[cid]
            if lop.Err == "" {
              log.Fatal("copyDB. Err")
            }

            if !ok || lop.Rpcid > lastOp[cid].Rpcid {
              lastOp[cid] = lop

            }
          }
          if reccnt == repcnt {
            done = true
          }
        default :          
          time.Sleep(100 * time.Millisecond)
          waitcnt ++
//          if waitcnt > 100 {
//            log.Fatal("Waiting for too long!!!")
//          }
      }
    }
  }
  kv.mu.Lock()
  return mdb, lastOp
}

// Execute the Ops until theop is executed or a reconfig is encountered.
// return (Value/PreviousValue, OK) if theop is executed.
// return ("", "Reconfig") if reconfig is encountered.
func (kv *ShardKV) execute(theop Op) (string, Err) {
  done := false
  to := 10 * time.Millisecond
  value := ""
  var err Err
  for !done {
    kv.px.Start(kv.exeseq, theop)
    for {
      decided, decop := kv.px.Status(kv.exeseq)
      trycnt := 0
      if decided {
        dop := decop.(Op)
        shardid := key2shard(dop.Key)
        kv.exeseq ++
        if dop.Type == "Put" {
          preValue, ok := kv.db[shardid][dop.Key]
          if !ok {
            kv.db[shardid][dop.Key] = ""
            preValue = ""
          }
          if dop.DoHash {
            kv.db[shardid][dop.Key] = strconv.Itoa(int(hash(preValue + dop.Value)))
          } else {
            kv.db[shardid][dop.Key] = dop.Value
          }
          value = preValue
          err = OK
        } else if dop.Type == "Get" {
          v, ok := kv.db[shardid][dop.Key]
          value = v
          if ok {
            err = OK
          } else {
            err = ErrNoKey
          }
        } else if dop.Type == "Reconfig" {
          if kv.config.Num != dop.Preconfig.Num {
            log.Fatal("Beforeconfig does not match.")
          }
          if kv.config.Num + 1 != dop.Afterconfig.Num {
            log.Fatal("Afterconfig is wrong.")
          }
          // update the current config. 
          // update kv.db
          // update kv.lastOp
          kv.config = dop.Afterconfig
          for sid, shard := range dop.Db {
            kv.db[sid] = shard
          }
          for cid, lastOp := range dop.LastOp {
            _, ok := kv.lastOp[cid]
            if !ok || kv.lastOp[cid].Rpcid < lastOp.Rpcid {
              kv.lastOp[cid] = lastOp
            }
          }
          err = ErrPendReconfig
        }
        if dop.Type == "Get" || dop.Type == "Put" {
          _, ok := kv.lastOp[dop.Cid]
          if ok && dop.Rpcid <= kv.lastOp[dop.Cid].Rpcid {
            log.Fatal("Missing Op too old")
          }
          kv.lastOp[dop.Cid] = LastOp{dop.Rpcid, dop, err, value}
        }
        if dop.Cid == theop.Cid && dop.Rpcid == theop.Rpcid || dop.Type == "Reconfig" {
          done = true
        }
        break
      } else {
        time.Sleep(to)
        if to < time.Second {
          to *= 2
        } else {
          trycnt ++
//          if trycnt > 5 {
//            log.Fatal("[paxos] take too long to decide")
//          }
        }
      }
    }
  }
  if err == "" {
    log.Fatal("[execute] Err is empty")
  }
  return value, err
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

//  DPrintfCLR(2, "[Server Get] gid=%d,server=%d args=%+v", 
//    kv.gid, kv.me, args)
  
  err := kv.ValidReq(args.Me, args.Rpcid, args.Key)
  if err == ErrDupReq {
    lastOp := kv.lastOp[args.Me]
    reply.Err = lastOp.Err
    reply.Value = lastOp.Value
    return nil 
  } else if err != OK {
    reply.Err = err
    return nil
  }
  var op Op
  op.Type = "Get"
  op.Key = args.Key
  op.Cid = args.Me
  op.Rpcid = args.Rpcid

//  kv.propose(op)
  value, err := kv.execute(op)
  reply.Value = value
  reply.Err = err
  if err == "OK" {
    kv.lastOp[args.Me] = LastOp{args.Rpcid, op, err, value}
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  err := kv.ValidReq(args.Me, args.Rpcid, args.Key)
  if err == ErrDupReq {
    lastOp := kv.lastOp[args.Me]
    reply.Err = lastOp.Err
    reply.PreviousValue = lastOp.Value
    return nil
  } else if err != OK {
    reply.Err = err
    return nil
  }
 
  var op Op
  op.Type = "Put"
  op.Key = args.Key
  op.Value = args.Value
  op.DoHash = args.DoHash
  op.Cid = args.Me
  op.Rpcid = args.Rpcid

  value, err := kv.execute(op)
  reply.PreviousValue = value
  reply.Err = err
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  
  config := kv.sm.Query(-1)
  if config.Num > kv.config.Num {
    config = kv.sm.Query(kv.config.Num + 1)
    if config.Num != kv.config.Num + 1 {
      log.Fatal("sm.Query does not work")
    }
    var op Op
    op.Type = "Reconfig"
    op.Preconfig = kv.config
    op.Afterconfig = config

    // copy the db and lastOp information from a remote group
    op.Db, op.LastOp = kv.copydb(kv.config, config)
    if op.Preconfig.Num == kv.config.Num {
      kv.execute(op)
      if kv.config.Num != config.Num {
        log.Fatal("New Config not working\n\tconfig=", config, 
          "\n\tkv.config", kv.config)
      }
    }
  }
  kv.mu.Unlock()
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
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{}) 
  gob.Register(make(map[int]map[string]string))
  gob.Register(LastOp{}) 
  gob.Register(make(map[int64]LastOp))


  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.lastOp = make(map[int64]LastOp)
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
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
