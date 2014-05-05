package shardkv

import "time"
import "sync"
import "strconv"
import "os"
import "log"
import "bytes"
import "encoding/gob"

const Persistent = true

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  groups map[int64][]string // gid -> servers[]
  gids []int64
  txnid int
  me int
  rpcid int
  curtxn * CurTxn
}

type CurTxn struct { 
  txnid int
  txns map[int64]*TxnArgs // txns[group_id] -> TxnArgs
  phase string // "Started", "Locked", "Prepared", "Done"
  prepare_ok bool
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
  ck.txnid = me
  ck.me = me
  ck.rpcid = 0

  if ck.LoadState() {
    ck.txnid = ck.curtxn.txnid
    ck.runCurTxn("")
  }
  return ck
}

// statically map a shard to a group
func (ck *Clerk) shard2group(shard int) int64 {
  return ck.gids[ shard % len(ck.gids) ]
}

func (ck *Clerk) LockGroups(txns map[int64]*TxnArgs) {
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
          time.Sleep(100 * time.Millisecond)
        }
      }
    }
  }
}

func (ck *Clerk) PrepareGroups(txns map[int64]*TxnArgs) (bool, []ReqReply) {
  results := make([]ReqReply, 0)
  //
  // All groups are locked.
  // Periodically send out Prepare requests.
  prepare_ok := true
  for _, gid := range ck.gids {
    _, ok := txns[gid]
    if ok {
      args := PrepArgs{ck.txnid, 0, ck.me}
      var reply PrepReply
      group_prepare_ready := false
      for !group_prepare_ready {
        for _, srv := range ck.groups[gid] {
          ok := call(srv, "ShardKV.Prepare_handler", &args, &reply)
          if ok && reply.Err == OK {
            group_prepare_ready = true
            if !reply.Prepare_ok {
              prepare_ok = false
            }
            results = append(results, reply.Replies...) 
            break
          }
          time.Sleep(100 * time.Millisecond)
        }
      }
    }
  }
  return prepare_ok, results
}

func (ck *Clerk) CommitGroups(txns map[int64]*TxnArgs, prepare_ok bool) {
  for _, gid := range ck.gids {
    _, ok := txns[gid]
    if ok {
      args := CommitArgs{ck.txnid, prepare_ok, 0, ck.me}
      var reply CommitReply
      committed := false
      for !committed {
        for _, srv := range ck.groups[gid] {
          ok := call(srv, "ShardKV.Commit_handler", &args, &reply)
          if ok && reply.Err == OK {
            committed = true
            break
          }
          time.Sleep(100 * time.Millisecond)
        }
      }
    }
  }
}

func (ck *Clerk) MakePersistent() {
  filename := "./client_persistent/client"+strconv.Itoa(ck.me)+".txt"
  f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    log.Fatal(err)
  }
  buf := new(bytes.Buffer)
  enc := gob.NewEncoder(buf)
  enc.Encode(ck.curtxn)
  f.Write(buf.Bytes())
  f.Close()
}
//
// returns whether the client state can be found on the disk
func (ck *Clerk) LoadState() bool {
  filename := "./client_persistent/client"+strconv.Itoa(ck.me)+".txt"
  if _, err := os.Stat(filename); err == nil {
    f, err := os.Open(filename)
    if err != nil {
      log.Fatal(err)
    }
    ck.curtxn = new(CurTxn)
    dec:= gob.NewDecoder(f)
    err = dec.Decode(ck.curtxn)
    f.Close()
    return true
  }
  return false
}

func (ck *Clerk) RunTxn(reqs []ReqArgs, failpoint string) (bool, []ReqReply) {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  
  // 
  // assign the requests to their corresponding groups
  ck.curtxn = new(CurTxn)
  ck.curtxn.txnid = ck.txnid
  ck.curtxn.phase = "Started"
  ck.curtxn.txns = make(map[int64]*TxnArgs)
  for _, req := range reqs {
    gid := ck.shard2group( key2shard(req.Key) )
    _, ok := ck.curtxn.txns[gid]
    if !ok {
      ck.curtxn.txns[gid] = new(TxnArgs)
      ck.curtxn.txns[gid].Txn_id = ck.txnid
    } 
    ck.curtxn.txns[gid].Txn = append(ck.curtxn.txns[gid].Txn, req)
  }
  
  return ck.runCurTxn(failpoint)
}

func (ck *Clerk) runCurTxn(failpoint string) (bool, []ReqReply) {
  // 
  // send out the lock requests
  DPrintfCLR(1, "[Clerk.runCurTxn] Will run txn %d. phase=%v", ck.curtxn.txnid, ck.curtxn.phase) 
  
  if ck.curtxn.phase == "Started" {
    ck.LockGroups(ck.curtxn.txns)
    ck.curtxn.phase = "Locked"
    if Persistent {
      ck.MakePersistent()
    }
  }

  DPrintfCLR(1, "[Clerk.runCurTxn] groups locked, start prepare phase") 
  
  var prepare_ok bool
  var results []ReqReply
  if ck.curtxn.phase == "Locked" {
    prepare_ok, results = ck.PrepareGroups(ck.curtxn.txns)
    ck.curtxn.prepare_ok = prepare_ok 
    if failpoint == "BeforeDiskWrite" {
      return false, nil
    }
    ck.curtxn.phase = "Prepared"
    if Persistent {
      ck.MakePersistent()
    }
  }
 
  if failpoint == "AfterDiskWrite" {
    return false, nil
  }

  DPrintfCLR(1, "[Clerk.runCurTxn] groups prepared, start commit phase") 
  
  if ck.curtxn.phase == "Prepared" {
    ck.CommitGroups(ck.curtxn.txns, prepare_ok)
  }
  ck.txnid += 100
  
  return prepare_ok, results
}
