package shardkv

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import "log"
//import "sync"
//import "math/rand"

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "skv-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

/*
func mcleanup(sma []*shardmaster.ShardMaster) {
  for i := 0; i < len(sma); i++ {
    if sma[i] != nil {
      sma[i].Kill()
    }
  }
}
*/

func cleanup(sa [][]*ShardKV, gid []int64, nreplicas int, rmFiles bool) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].kill()
    }
  }
	
  if rmFiles {
    for i := 0; i < len(gid); i++ {
      for j := 0; j < nreplicas; j++ {
        if err := os.RemoveAll(dirname(gid[i],j)); err != nil{
          log.Fatal("RemoveDirs Failed")
        }
      }
    }
    if err := os.RemoveAll("client_persistent"); err != nil {
      log.Fatal("RemoveDirs Failed")
    }
    if err := os.RemoveAll("paxos_log"); err != nil {
      log.Fatal("RemoveDirs Failed")
    }

  }
}

func check(db map[string]string, ck *Clerk) {
  reqs := make([]ReqArgs, len(db))
  i := 0
  for key, _ := range db {
    reqs[i].Type = "Get"
    reqs[i].Key = key
    reqs[i].Value = ""
    i ++
  }
  commit, results := ck.RunTxn(reqs, "")
  if !commit {
    log.Fatal("Read only txns not committed.")
  }
  if len(db) != len(results) {
    log.Fatal("results length is not correct")
  }

  for _, req_reply := range results {
    key := req_reply.Key
    value := req_reply.Value
    if value != db[key] {
      fmt.Printf("db = %+v\n reply = %+v\n", db, results)
      log.Fatal("value does not match")
    }
  }
}

func param_setup(tag string, unreliable bool, ngroups int, nreplicas int, failpoint string) ([]int64, [][]string, [][]*ShardKV, func(rmFiles bool)) {

  runtime.GOMAXPROCS(4)

  gids := make([]int64, ngroups)    // each group ID
  ha := make([][]string, ngroups)   // ShardKV ports, [group][replica]
  sa := make([][]*ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      if i == 0 {
        sa[i][j] = StartServer(gids[i], ha[i], j, true, failpoint)
      } else {
        sa[i][j] = StartServer(gids[i], ha[i], j, true, "")
      }
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func(rmFiles bool) { cleanup(sa, gids, nreplicas, rmFiles) } // ; mcleanup(sma) }
  return gids, ha, sa, clean

}

func setup(tag string, unreliable bool, persistent bool) ([]int64, [][]string, [][]*ShardKV, func(rmFiles bool)) {
  runtime.GOMAXPROCS(4)

  const ngroups = 3   // replica groups
  const nreplicas = 3 // servers per group
  gids := make([]int64, ngroups)    // each group ID
  ha := make([][]string, ngroups)   // ShardKV ports, [group][replica]
  sa := make([][]*ShardKV, ngroups) // ShardKVs
  // defer cleanup(sa)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    sa[i] = make([]*ShardKV, nreplicas)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      ha[i][j] = port(tag+"s", (i*nreplicas)+j)
    }
    for j := 0; j < nreplicas; j++ {
      sa[i][j] = StartServer(gids[i], ha[i], j, persistent, "")
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func(rmFiles bool) { cleanup(sa, gids, nreplicas, rmFiles) } // ; mcleanup(sma) }
  return gids, ha, sa, clean
}


func txnAbort(t *testing.T, unreliable bool) {
  gids, ha, _, clean := setup("basic", unreliable, false)
  defer clean(true)

  fmt.Printf("Test: Single Client. Abort should roll back.\n")
  
  db := make(map[string]string)
  groups := make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  ck := MakeClerk(0, groups)
  
  // Txn 1
  reqs := make([]ReqArgs, 10)
  for i := 0; i < 10; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(2)
    db[ reqs[i].Key ] = reqs[i].Value
  }
  commit, value := ck.RunTxn(reqs, "")
  fmt.Printf("commit=%v\nvalue=%+v\n", commit, value)
  // check results
  check(db, ck) 

  reqs = make([]ReqArgs, 3)
  reqs[0] = ReqArgs{"Put", "1", "2"}
  reqs[1] = ReqArgs{"Add", "2", "-3"}
  reqs[2] = ReqArgs{"Put", "3", "2"}
  commit, value = ck.RunTxn(reqs, "")

  if commit {
    log.Fatal("Should not commit")
  }

  check(db, ck)
  fmt.Printf("  ... Passed\n")
}

func TestTxnAbort(t *testing.T) {
  txnAbort(t, false)
}
func TestTxnAbortUnreliable(t *testing.T) {
  txnAbort(t, true)
}

func txnConcurrent(t *testing.T, unreliable bool) {
  gids, ha, _, clean := setup("basic", unreliable, false)
  defer clean(true)

  fmt.Printf("Test: Three Client. Abort should roll back.\n")
  //db := make(map[string]string)
  
  groups := make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  
  Ncli := 3
  
  ck := make([]*Clerk, Ncli)
  for i := 0; i < Ncli; i++ {
    ck[i] = MakeClerk(i, groups);
  }
  
  // Txn 1
  reqs := make([]ReqArgs, 10)

  for i := 0; i < 10; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(30)
  }
  
  ck[0].RunTxn(reqs, "")

  for i := 0; i < 10; i++{
    reqs[i].Type = "Get"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(30)
  }

  _, replies := ck[0].RunTxn(reqs, "")

  for i := 0; i < 10; i++{
    if replies[i].Value != "30" {
      log.Fatalf("Error: value is not put correctly\n")
    }
  }

  for i := 0; i < 10; i++{
    reqs[i].Type = "Add"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(-1)
  }

  
  valueTouched := make([]bool, 30)

  for i := 0; i < 30; i++{
    valueTouched[i] = false
  }
  
  ca := make([]chan bool, Ncli)
  
  for iter := 0; iter < 10; iter++ {
    for cli := 0; cli < Ncli; cli++ {
      ca[cli] = make(chan bool)
      go func(me int) {
        defer func() {ca[me] <- true}()
        ok, txnReply := ck[me].RunTxn(reqs, "")
        if ok {
          for i := 0; i < 9; i++ {
            if txnReply[i].Value != txnReply[i+1].Value {
              log.Fatalf("Error: add fails, values are not same\n")
            }
          }
          
          ind, _ := strconv.Atoi(txnReply[0].Value)
          if !valueTouched[ind] {
            valueTouched[ind] = true
          } else {
            log.Fatalf("Error: add fails, this value has already been added\n")
          }
        } else {
          log.Fatalf("Error: should not abort\n")
        }
      }(cli)
    }
    
    for i := 0; i < Ncli; i++ {
      <- ca[i]
    }
  }
  fmt.Printf("  ... Passed\n")
}

func TestTxnConcurrent(t *testing.T) {
  txnConcurrent(t, false)
}
func TestTxnConcurrentUnreliable(t *testing.T) {
  txnConcurrent(t, true)
}

func dbPersistent(t *testing.T, unreliable bool) {
	gids, ha, _, clean := setup("basic", unreliable, true)
  //defer clean(true)

  fmt.Printf("Test: Single Client. Abort should roll back. XXXXXXXXXXXXXX\n")
  
  db := make(map[string]string)
  
  groups := make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  ck := MakeClerk(0, groups)
  
  reqs := make([]ReqArgs, 10)
  for i := 0; i < 10; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(1)
    db[ reqs[i].Key ] = reqs[i].Value
  }
  ck.RunTxn(reqs, "")
  // check results
  check(db, ck) 
  clean(false)

  gids, ha, _, clean = setup("basic", unreliable, true)
  defer clean(true)
	
  groups = make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  ck = MakeClerk(0, groups)

  check(db, ck)
  
  fmt.Printf("  ... Passed\n")
}

func TestDbPersistent(t *testing.T) {
  dbPersistent(t, false)
}

func Test2PCClientCrash(t *testing.T) {  

//  gids, ha, _, clean := setup("basic", false, false)
  gids, ha, _, clean := param_setup("basic", false, 3, 1, "")
  defer clean(true)

  fmt.Printf("Test: Different failure points for client\n")
   
  db := make(map[string]string)
  groups := make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  ck := MakeClerk(0, groups)
  
  // initialize database
  reqs := make([]ReqArgs, 10)
  for i := 0; i < 10; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(2)
    db[ reqs[i].Key ] = reqs[i].Value
  }
  ck.RunTxn(reqs, "")
  check(db, ck) 

  // run Txn 
  reqs = make([]ReqArgs, 3)
  for i := 0; i < 3; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(1)
    db[ reqs[i].Key ] = reqs[i].Value
  }
  commit, _ := ck.RunTxn(reqs, "BeforeDiskWrite")
  if commit {
    log.Fatal("should not commit if the under crash")
  } 

  commit, _ = ck.Reboot()
  if !commit {
    log.Fatal("txn does not commit after reboot")
  } 
  check(db, ck) 
  fmt.Printf("  ... Passed\n")
}

func serverCrash(t *testing.T, failpoint string) {
  gids, ha, sa, clean := param_setup("basic", false, 3, 1, failpoint)
  defer clean(true)

  fmt.Printf("Test: server fails at %s\n", failpoint)
   
  db := make(map[string]string)
  groups := make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  ck := MakeClerk(0, groups)
  
  // initialize database
  reqs := make([]ReqArgs, 10)
  for i := 0; i < 10; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(0)
    db[ reqs[i].Key ] = reqs[i].Value
  }
  commit_chan := make(chan bool)
  go func() {
    commit, _ := ck.RunTxn(reqs, "")
    commit_chan <- commit
  }()
  restart := false
  sleep_cnt := 0
  select {
  case commit := <-commit_chan :
    if restart == false {
      log.Fatal("server failed. the client should not commit.")
    }
    if !commit {
      log.Fatal("commit failed")
    }
  default:
    time.Sleep(100 * time.Millisecond)
    sleep_cnt ++
    if sleep_cnt >= 10 {
      restart = true
      sa[0][0].Reboot()
    }
  }
  check(db, ck) 
}

func Test2PCServerCrash(t *testing.T) {  
  serverCrash(t, "BeforePrepare")
  serverCrash(t, "AfterPrepare")
  serverCrash(t, "BeforeCommit")
  serverCrash(t, "AfterCommit")
}
