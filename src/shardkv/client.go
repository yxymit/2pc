package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  config shardmaster.Config
  groups map[int64][]string // gid -> servers[]
  txnid int
  me int
  rpcid int
}

func MakeClerk(me int, groups map[int64][]string) *Clerk {
  ck := new(Clerk)
  ck.groups = groups
  ck.txnid = 0
  ck.me = me
  ck.rpcid = 0
  return ck
}


func RunTxn(reqs []TxnArgs) {
  var args InsOpArgs 
  args.txn_id = txnid * 100 + ck.me
  for _, req := range reqs {
     
     args.txn.PushBack() 
  }
  type InsOpArgs struct {
    txn_id int
    txn list.List
  }
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Get().
  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := GetArgs{key, ck.me, ck.rpcid}
        var reply GetReply
        ok := call(srv, "ShardKV.Get", &args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          ck.rpcid ++
          DPrintfCLR(3, "[client Get Done] key=%s, value=%s, gid=%d, server=%v", key, reply.Value, gid, srv)
          return reply.Value
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
  return ""
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]
    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := PutArgs{key, value, dohash, ck.me, ck.rpcid}
        var reply PutReply
        ok := call(srv, "ShardKV.Put", &args, &reply)

        if ok && reply.Err == OK {
          ck.rpcid ++
          DPrintfCLR(1, "[client Put Done] key=%s, value=%s, gid=%d, server=%v", key, reply.PreviousValue, gid, srv)
          return reply.PreviousValue
        } 
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
    // ask master for a new configuration. 
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
