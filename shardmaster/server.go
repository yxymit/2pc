package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "container/list"
import "time"
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
    log.Printf(CLR+format+CLR_N, a...)
  }
  return
}

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config      // indexed by config num
  maxConfigNum int
  seq int               // paxos seq
}


type Op struct {
  // Your data here.
  Type string       // Join | Leave | Move | Query
  GID int64         // 
  Servers []string  // for Join
  Shard int         // for Move
  Me int            // for Query
}

func (sm *ShardMaster) balance(conf *Config) {
  shardsPerGroup := make(map[int64]*list.List) // gid to a list of shards
  var lastshards *list.List
  for i, _ := range conf.Groups {
    shardsPerGroup[i] = new(list.List)
    lastshards = shardsPerGroup[i]
  }
  for i, v := range conf.Shards {
    _, ok := shardsPerGroup[v]
    if ok {
      shardsPerGroup[v].PushBack(i)
    } else {
      lastshards.PushBack(i)
    }
  }
  groupNum := len(conf.Groups)
  min := NShards / groupNum
  max := NShards / groupNum
  if NShards % groupNum != 0 {
    max ++
  }
  // move shards from the max group to min group.
  done := false
  for !done  {
    var maxgid, mingid int64
    maxshards := 0
    minshards := NShards
    for gid, shards := range shardsPerGroup {
      if shards.Len() < minshards {
        minshards = shards.Len()
        mingid = gid
      }
      if shards.Len() > maxshards {
        maxshards = shards.Len()
        maxgid = gid
      }
    }
    // unbalanced. Rebalance
    if maxshards > max || minshards < min {
      delta := maxshards - min
      delta2 := max - minshards
      if delta > delta2 {
        delta = delta2
      }
      for i := 0; i < delta; i++ {
        e := shardsPerGroup[maxgid].Front()
        shardsPerGroup[maxgid].Remove(e)
        shardsPerGroup[mingid].PushBack(e.Value.(int))
      }
    } else {
      done = true
    }
  }
  // reconstruct Config.Shards
  for pid, shards := range shardsPerGroup {
    if shards.Len() < min {
      log.Fatal("No shards in group")
    }
    for e := shards.Front(); e != nil; e=e.Next() {
      conf.Shards[e.Value.(int)] = pid
    }
  }
}

func (sm *ShardMaster) request(optype string, args interface{}) {

  var op Op
  op.Type = optype
  op.GID = -1
  op.Servers = nil
  op.Shard = -1
  if optype == "Join" {
    op.GID = args.(JoinArgs).GID
    op.Servers = args.(JoinArgs).Servers
  }else if optype == "Leave" {
    op.GID = args.(LeaveArgs).GID
  } else if optype == "Move" {
    op.GID = args.(MoveArgs).GID
    op.Shard = args.(MoveArgs).Shard
  } else if optype == "Query" {
    op.Me = sm.me
  }

  done := false
  for !done {
    sm.px.Start(sm.seq, op)
    decided, decop := sm.px.Status(sm.seq)
    for !decided {
      time.Sleep(10 * time.Millisecond)
      decided, decop = sm.px.Status(sm.seq)
    }
    decoper := decop.(Op)
    sm.seq ++
    if decoper.Type == "Query" {
      if decoper.Type == op.Type && decoper.Me == op.Me {
        return
      } else {
        continue
      }
    }
    // execute the decided operation
    lastConfig := sm.configs[sm.maxConfigNum]
    var newConfig Config
    newConfig.Num = lastConfig.Num + 1
    newConfig.Shards = lastConfig.Shards
    newConfig.Groups = make(map[int64][]string)
    for k, v := range lastConfig.Groups {
      newConfig.Groups[k] = v
    }
    if decoper.Type == "Join" {
      newConfig.Groups[decoper.GID] = decoper.Servers
      sm.balance(&newConfig)
    } else if decoper.Type == "Leave" {
      delete(newConfig.Groups, decoper.GID)
      sm.balance(&newConfig)
    } else if decoper.Type == "Move" {
      newConfig.Shards[decoper.Shard] = decoper.GID
    }
//    DPrintfCLR(5, "[Decided] seq=%d, Me=%d\ndecop=%+v\nOld Config=%+v\nNew Config=%+v\n", 
//      sm.seq, sm.me, decoper, sm.configs[sm.maxConfigNum], newConfig)
    sm.configs = append(sm.configs, newConfig)
    if sm.configs[newConfig.Num].Num != newConfig.Num {
      log.Fatal("Config Num wrong")
    }
    sm.maxConfigNum ++
    if decoper.Type == op.Type && 
        decoper.GID == op.GID &&
        decoper.Shard == op.Shard {
      done = true
    }
  }
  sm.px.Done(sm.seq - 1)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
//  DPrintfCLR(1, "[Join]\n")
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  sm.request("Join", *args)
//  DPrintfCLR(1, "[Join] args=%+v\n%+v\n", *args, sm.configs[sm.maxConfigNum])
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
//  fmt.Println("[Leave]", args)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  sm.request("Leave", *args)
//  DPrintfCLR(2, "[Leave] args=%+v\n%+v\n", *args, sm.configs[sm.maxConfigNum])
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
//  fmt.Println("[Move]", args)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.request("Move", *args)
//  DPrintfCLR(3, "[Move] args=%+v\n%+v\n", *args, sm.configs[sm.maxConfigNum])
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  num := args.Num
//  DPrintfCLR(6, "[Query] args=%+v, sm.maxConfigNum=%d", *args, sm.maxConfigNum)
  if num != -1 && num < sm.maxConfigNum {
    reply.Config = sm.configs[num]
  } else {
    sm.request("Query", *args)
    reply.Config = sm.configs[sm.maxConfigNum]
  }
  if num != reply.Config.Num && num != -1{
//    DPrintfCLR(5, "[Query] args=%+v\n\tconfigs=%+v\n", *args, sm.configs)
//    DPrintfCLR(5, "[Query] args=%+v, sm.maxConfigNum=%d\n", 
//      *args, sm.maxConfigNum)
//    DPrintfCLR(5, "[Query] args=%+v\n\tconfigs[num]=%+v\n", *args, sm.configs[num])
    log.Fatal("Query reply wrong!")
  }
//  DPrintfCLR(4, "[Query] args=%+v\n%+v\n", *args, reply)
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)
  sm.maxConfigNum = 0
  sm.seq = 0

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
