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
  gids []int64
  txnid int
  me int
  rpcid int
}

func MakeClerk(me int, groups map[int64][]string) *Clerk {
  ck := new(Clerk)
  ck.groups = groups
  ck.gids := make([]int64, len(groups))
  i := 0
  for k, _ := range groups {
    gids[i] = k
    i++
  }
  ck.txnid = 0
  ck.me = me
  ck.rpcid = 0
  return ck
}

// statically map a shard to a group
func (ck *Clerk) shard2group(shard int) int64 {
  return gids[ shard % len(ck.gids) ]
}

func (ck *Clerk) SendTxns(reqs []ReqArgs) {
  var txns map[int64]*TxnArgs // [gid] -> *TxnArgs
  for _, req := range reqs {
    gid := ck.shard2group(key2shard(req.Key))
    _, ok := txns[gid]
    if !ok {
      txns[gid] = new(TxnArgs)
      txns[gid].Txn_id = ck.txnid * 100 + ck.me
      txns[gid].Txn.PushBack(req)
    }
  }
  // Must lock the groups in a fixed order
  for _, gid := range gids {
    txn, ok := txns[gid]
    if ok {  // the gid is involved in this txn. send request
      for _, srv := range groups[gid] {
        var reply TxnReply 
        ok := call(srv, "ShardKV.XXX", txn, &reply)
        if ok && (reply.Err == OK) {
          // the current group is successfully locked. 
          // Proceed to the next group
          break
        }
      }
    }
  }
}

func (ck *Clerk) RunTxn(reqs []ReqArgs) {
  SendTxns( reqs )
  // All groups are locked.
  // Periodically send out Prepare requests.
  // TODO
  ck.txnid ++
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
