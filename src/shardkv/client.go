package shardkv

import "shardmaster"
//import "time"
import "sync"

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
    ck.gids[i] = k
    i++
  }
  ck.txnid = 0
  ck.me = me
  ck.rpcid = 0
  return ck
}

// statically map a shard to a group
func (ck *Clerk) shard2group(shard int) int64 {
  return ck.gids[ shard % len(ck.gids) ]
}

func (ck *Clerk) RunTxn(reqs []ReqArgs) (bool, []ReqReply) {
  //
  // partition the requests based on groups
//  var txns map[int64]*TxnArgs // [gid] -> *TxnArgs
  txns := make(map[int64]*TxnArgs)
  txnid := ck.txnid * 100 + ck.me
  for _, req := range reqs {
    gid := ck.shard2group( key2shard(req.Key) )
    _, ok := txns[gid]
    if !ok {
      txns[gid] = new(TxnArgs)
      txns[gid].Txn_id = txnid
    } 
    txns[gid].Txn = append(txns[gid].Txn, req)
  }
  
  DPrintfCLR(1, "Ready to send out Lock requests")
  //
  // send the requests belong to a group to that group 
  // Must lock the groups in a fixed order
  for _, gid := range ck.gids {
    txn, ok := txns[gid]
    if ok {  // the gid is involved in this txn. send request
      locked := false
      for !locked {
        for _, srv := range ck.groups[gid] {
          var reply TxnReply 
          ok := call(srv, "ShardKV.Insert_txn", txn, &reply)
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
  DPrintfCLR(1, "All groups are locked. Ready to send Prepare requests")
  results := make([]ReqReply, 0)
  //
  // All groups are locked.
  // Periodically send out Prepare requests.
  prepare_ok := true
  for _, gid := range ck.gids {
    _, ok := txns[gid]
    if ok {
      args := PrepArgs{txnid, 0, ck.me}
      var reply PrepReply
      group_prepare_ready := false
      for !group_prepare_ready {
        for _, srv := range ck.groups[gid] {
          ok := call(srv, "ShardKV.Prepare_handler", &args, &reply)
          if ok && reply.Err == OK {
            DPrintfCLR(2, "gid=%d. prepare_ok=%v", gid, reply.Prepare_ok)
            group_prepare_ready = true
            if !reply.Prepare_ok {
              prepare_ok = false
            }
            results = append(results, reply.Replies...) 
            break
          }
        }
      }
    }
  }

  DPrintfCLR(1, "Prepares returned. prepare_ok=%v", prepare_ok)
  // All Prepare results are back. send out the second phase commit/abort message.
  for _, gid := range ck.gids {
    _, ok := txns[gid]
    if ok {
      args := CommitArgs{txnid, prepare_ok, 0, ck.me}
      var reply CommitReply
      committed := false    
      for !committed {
        for _, srv := range ck.groups[gid] {
          ok := call(srv, "ShardKV.Commit_handler", &args, &reply)
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
