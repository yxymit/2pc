package shardkv

import "testing"
import "runtime"
import "strconv"
import "os"
//import "time"
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

func cleanup(sa [][]*ShardKV) {
  for i := 0; i < len(sa); i++ {
    for j := 0; j < len(sa[i]); j++ {
      sa[i][j].kill()
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
  commit, results := ck.RunTxn(reqs)
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
      log.Fatal("value does not match")
    }
  }
}

func setup(tag string, unreliable bool) ([]int64, [][]string, [][]*ShardKV, func()) {
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
      sa[i][j] = StartServer(gids[i], ha[i], j)
      sa[i][j].unreliable = unreliable
    }
  }

  clean := func() { cleanup(sa) } // ; mcleanup(sma) }
  return gids, ha, sa, clean
}

func TestTxnAbort(t *testing.T) {
  gids, ha, _, clean := setup("basic", false)
  defer clean()

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
    reqs[i].Value = strconv.Itoa(1)
    db[ reqs[i].Key ] = reqs[i].Value
  }
  commit, value := ck.RunTxn(reqs)
  fmt.Printf("commit=%v\nvalue=%+v\n", commit, value)
  if commit {
    log.Fatal("Should not commit")
  }

  // check results
  check(db, ck) 
   
  reqs = make([]ReqArgs, 3)
  reqs[0] = ReqArgs{"Put", "1", "2"}
  reqs[1] = ReqArgs{"Add", "2", "-2"}
  reqs[2] = ReqArgs{"Put", "3", "2"}
  ck.RunTxn(reqs)

  check(db, ck)
  fmt.Printf("  ... Passed\n")
}

func TestTxnConcurrent(t *testing.T) {
  gids, ha, _, clean := setup("basic", false)
  defer clean()

  fmt.Printf("Test: Three Client. Abort should roll back.\n")
  
  //db := make(map[string]string)
  
  groups := make(map[int64][]string)
  for i, gid := range gids {
    groups[gid] = ha[i]
  }
  
	Ncli := 3
	
	var ck [NCli]*Clerk
	for i: = 0; i < Ncli; i++ {
		ck[i] := MakeClerk(i, groups);
	}
  
  // Txn 1
  reqs := make([]ReqArgs, 10)
  for i := 0; i < 10; i++ {
    reqs[i].Type = "Put"
    reqs[i].Key = strconv.Itoa(i)
    reqs[i].Value = strconv.Itoa(30)
  }
  
	ck[0].RunTxn(reqs)

	for i := 0; i < 10; i++{
		reqs[i].Type = "Get"
		reqs[i].Key = strconv.Itoa(i)
		reqs[i].Value = strconv.Itoa(30)
	}

	_, replies := ck[0].RunTxn(reqs)

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
	
	var ca [Ncli]chan bool
	
	for iter := 0; iter < 10; i++ {
		for cli := 0; cli < Ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				defer func() {ca[me] <- true}
				ok, txnReply := ck[i].RunTxn(reqs)
				if ok {
					for i := 0; i < 9; i++ {
						if txnReply[i].Value != txnReply[i+1].Value {
							log.Fatalf("Error: add fails, values are not same\n")
						}
					}
					if !valueTouched[strconv.Atoi(txnReply[i].Value)] {
						valueTouched[strconv.Atoi(txnReply[i].Value)] = true
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
