package shardkv

import "shardmaster"
//import "time"
import "container/list"
import "sync"
//import "fmt"

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
  ck.gids = make([]int64, len(groups))
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
  // Must lock the groups in a fixed order
  for _, gid := range ck.gids {
    txn, ok := ck.txns[gid]
    if ok {  // the gid is involved in this txn. send request
      locked := false
      for !locked {
        for _, srv := range groups[gid] {
          var reply TxnReply 
          ok := call(srv, "ShardKV.XXX", txn, &reply)
          if ok && (reply.Err == OK) {
            // the current group is successfully locked. 
            // Proceed to the next group
            locked = true
            break
          }
        }
      }
    }
  }
}

func (ck *Clerk) RunTxn(reqs []ReqArgs) (bool, list.List) {
  
  var txns map[int64]*TxnArgs // [gid] -> *TxnArgs
  txnid := ck.txnid * 100 + ck.me
  for _, req := range reqs {
    gid := ck.shard2group( key2shard(req.Key) )
    _, ok := txns[gid]
    if !ok {
      txns[gid] = new(TxnArgs)
      txns[gid].Txn_id = txnid
      txns[gid].Txn.PushBack(req)
    }
  }

  // send txn requests to all involved groups.
  SendTxns( reqs )
  
  results := make(list.List)
  // All groups are locked.
  // Periodically send out Prepare requests.
  prepare_ready := false
  prepare_ok := true
  for _, gid := range ck.gids {
    txn, ok := txns[gid]
    if ok {
      args := PrepArgs{txnid, 0, ck.me}
      var reply PrepReply
      group_prepare_ready := false
      for !group_prepare_ready {
        for _, srv := range groups[gid] {
          ok := call(srv, "ShardKV.XXX", &args, &reply)
          if ok && reply.Err == OK {
            group_prepare_ready = true
            if !reply.Prepare_ok {
              prepare_ok = false
            }
            results.PushBackList(reply.Replies) 
            break
          }
        }
      }
    }
  }

  // All Prepare results are back. send out the second phase commit/abort message.
  for _, gid := range gids {
    txn, ok := txns[gid]
    if ok {
      args := CommitArgs{txnid, prepare_ok, ck.me}
      var reply CommitReply
      committed := false    
      for !commited {
        for _, srv := range groups[gid] {
          ok := call(srv, "ShardKV.XXX", &args, &reply)
          if ok && reply.Err == OK {
            committed = true
            break
          }
        }
      }
    }
  }
  ck.txnid ++
  return prepare_ok, results
}
