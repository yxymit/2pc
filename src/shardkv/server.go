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
  
	Type string // 'lock', 'prep', 'commit/unlock'
	Txn list.List
	Txn_id int
	Op_id string

	// for Type == prep
	Prepare_ok bool
	Reply_list list.List
	
	// for Type == commit

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

	// Transcation database here.
	dblock bool
	txn_id int
	curr_txn list.List
	reply_list list.List
}


// Execute the Ops until theop is executed or a reconfig is encountered.
// return (Value/PreviousValue, OK) if theop is executed.
// return ("", "Reconfig") if reconfig is encountered.
func (kv *ShardKV) insertPaxos(theop Op) Err {
  for {
		if kv.dblock && txn_id != theop.Txn_id {
			return ErrNoLock
		}

    kv.px.Start(kv.exeseq, theop)
		to := 10 * time.Millisecond
    for {
      decided, decop := kv.px.Status(kv.exeseq)
      if decided {
        dop := decop.(Op)
        kv.exeseq++
				
				switch dop.Type {
				case "Lock":
					kv.doLock()
				case "Prep":
					kv.doPrep()
				case "Commit":
					kv.doCommit()
				default:
				}

				if dop.Op_id == theop.Op_id {
					return
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

func (kv *ShardKV) doLock() bool {
	if !kv.dblock() {
		kv.dblock = true
		kv.txn_id = dop.Txn_id
		kv.curr_txn = dop.Txn
		/*
		send_kill := make(chan bool)
		recv_poll := make(chan Op)


		// poll from peers
		go func(send_kill chan bool, recv_poll chan Op){
			for {
				select {
				case <- send_kill:
					break
				default:
					decided, decop := kv.px.poll(kv.exeseq)
					if decided {
						recv_poll <- decop.(Op)
						break
					}
				}
			}(send_kill, recv_poll)
		}

		for {
			select {
			case theOp := <- recv_poll:
				// go to next phase
				break
			default:
				// next iteration
			}

		}
		*/
	}
}

func (kv *ShardKV) doPrep() bool {	
	if kv.dblock && kv.txn_id == dop.Txn_id{
		return true
	}
}

func (kv *ShardKV) doCommit() {
	if kv.dblock && kv.txn_id == dop.Txn_id{
		kv.dblock = false
	}
}


func (kv *ShardKV) insert_txn(args *InsOpArgs, reply *InsOpReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opid := strconv.Itoa(time.Now().Nanosecond())

	myop := Op{OpCode: "Lock", Txn: args.txn, Txn_id: args.txn_id}

	kv.insertPaxos(myop)

}

func (kv *ShardKV) prepare_handler(args *PrepArgs, reply *PrepReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	prepare_ok := true

	reply_list := list.New()

	for e := kv.curr_txn.Front(); e != nil; e = e.Next() {
		decOp := e.Value
		switch theop := decOp.(type) {
		default:
			log.Fatalf("Operation %T not supported by the database", theOp)
			prepare_ok = false

		case PutArgs:
			key := theop.Key
			new_val := theop.Value
			dohash := theop:DoHash
			
			shard_id := key2shard(key)
			
			curr_val := kv.db[shard_id][key]
			
			if kv.config.Shard[shard_id] == kv.gid {
				if dohash {
					if int(hash(curr_val + new_val)) < 0 {
						// puthash new_val is not integer
						prepare_ok = false
						break
					} 
				} else {
					i, err := strconv.Atoi(new_val)
					if err != nil {
						// put new_val is not integer
						log.Fatal(err)
						prepare_ok = false
						break
					} else {
						// put new_val is less than 0
						if i < 0 {
							prepare_ok = false
							break
						} 
					}
				}
				reply_list.PushBack(PutReply{Err: OK, PreviousValue: curr_val})
			} else {
				reply_list.PushBack(nil)
			}
		case GetArgs:
			if kv.config.Shard[shard_id] == kv.gid {
				reply_list.PushBack(GetReply{Err: OK, Value: curr_val})
			} else {
				reply_list.PushBack(nil)
			}
		}	
	}

	opid := strconv.Itoa(time.Now().Nanosecond())
	
	myop var Op
	
	if prepare_ok {
		myop = Op{OpCode: "Prep", Txn_id: args.txn_id, Prepare_ok: prepare_ok, Reply_list: reply_list}
	} else {
		myop = Op{Opcode: "Prep", Txn_id: args.txn_id, Prepare_ok: prepare_ok}
	}

	kv.insertPaxos(myop)

}

func (kv *ShardKV) commit_handler(args *CommitArgs, reply *CommitReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opid := strconv.Itoa(time.Now().Nanosecond())

	myop := Op{OpCode: "Commit", Txn_id: args.txn_id}

	kv.insertPaxos(myop)

}

func (kv *Shard) poll(){
	if kv.px.poll(kv.exeseq) {
		kv.execute()
	}
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

	kv.dblock = false

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
      //kv.tick()
			kv.poll()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}

