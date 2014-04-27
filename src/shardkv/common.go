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

type TxnArgs struct {
  Type string // Put, Get, Add
  Key string
  Value string
}

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Me int64
  Rpcid int
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Me int64
  Rpcid int
}

type GetReply struct {
  Err Err
  Value string
}

type AddArgs struct {
  Key string
  Me int64
  Rpcid int
  
  Value string
}

type CopyArgs struct {
  ConfigNum int
  Shardid int
}

type CopyReply struct {
  Err Err
  Shardid int
  Shard map[string]string
  LastOp map[int64]LastOp
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
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

type InsOpArgs struct {
  txn_id int
  txn list.List
}

type InsOpReply struct {
  Err Err
}

type PrepArgs struct {
  txn_id int
}

type PrepReply struct {
  Err Err
}

type CommitArgs struct {
  txn_id int
  commit bool
}

type CommitReply struct {
  Err Err
}
