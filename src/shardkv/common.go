package shardkv
import "hash/fnv"

import "container/list"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const NShards = 10

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrPendReconfig = "ErrPendReconfig"
  ErrDupReq = "ErrDupReq"
  ErrStaleReq = "ErrStaleReq"
  ErrNoLock = "ErrNoLock"
)
type Err string

type ReqArgs struct {
  Type string // Put, Get, Add
  Key string
  Value string
}

type ReqReply struct {
  Type string
  Key string
  Value string
}

type LastOp struct {
  Rpcid int
  Op Op
  // for return values
  Err Err
  Value string
}

type CopyData struct {
  db map[int]map[string]string
  lastOp map[int64]LastOp
}

type TxnArgs struct {
  Txn_id int
  Rpcid int
  Me int
  Txn list.List
}

type TxnReply struct {
  Err Err
}

type PrepArgs struct {
  Txn_id int
  Rpcid int
  Me int
}

type PrepReply struct {
  Err Err
  Replies list.List
}

type CommitArgs struct {
  Txn_id int
  Commit bool
  Rpcid int
  Me int
}

type CommitReply struct {
  Err Err
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}


