package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
//import "io"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "strconv"
import "bytes"
import "encoding/gob"

//const Persistent=false

type Instance struct {
  // States for Proposer & learner
  Decided bool
  // States for Accepter
  N_p int64      // highest prepare seen
  N_a int64      // highest accept seen
  V_a interface{} 
}

func (ins *Instance) Initialize() {
  ins.Decided = false
  ins.N_p = 0
  ins.N_a = 0
  ins.V_a = nil
}

type Paxos struct {
  mu sync.Mutex
  pmu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  name string

  // Your data here.
  instances map[int]*Instance // seq -> Instance mapping
  maxseq int
  minseq []int  // min seq from all peers

  persistent bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) Propose(seq int, v interface{}) {
  px.pmu.Lock()
  defer px.pmu.Unlock()
  
  px.mu.Lock()
  ins := px.getInstance(seq) 
  px.mu.Unlock()

  
  if ins == nil {
    return
  }
  for !px.dead && !ins.Decided && seq > px.minseq[px.me] {
    ts := time.Now().UnixNano()
    ts = ts * int64(len(px.peers)) + int64(px.me)
    // Send prepare requests to every peer
    proResp := make(chan PreReply)
    for pid := 0; pid < len(px.peers); pid++ {
      args := PreArgs{seq, ts}
      reply := PreReply{false, -2, nil}
      go func(pid int) {
        if pid == px.me {
          px.Prepare(&args, &reply)
          proResp <- reply
        } else {
          ok := call(px.peers[pid], "Paxos.Prepare", &args, &reply)
          if ok {
            proResp <- reply
          }
        }
      }(pid)
    }
    // collect majority responses
    respNum := 0
    var maxts int64
    maxts = 0
    minval := v
    delayNum := 0
    done := false
    for !done {
      select {
        case reply := <-proResp:
          if reply.Ok {
            respNum ++
            if reply.Ts > maxts && reply.Value != nil {
              maxts = reply.Ts
              minval = reply.Value
            }
            if respNum > len(px.peers) / 2 {
              done = true
            }
          }
        default:
          time.Sleep(DelayInterval)
          delayNum ++
          if delayNum >= DelayMaxNum {
           // Timeout, should retry
            done = true
          }
      }
    }
    if respNum <= len(px.peers) / 2 {
      ms := rand.Int31() % 1000
      time.Sleep( time.Duration(ms) * time.Millisecond )
      continue
    }
    // Already collected enough prepare_OK, 
    // should send out Accept requests
    accResp := make(chan AccReply)
    for pid := 0; pid < len(px.peers); pid++ {
      args := AccArgs{seq, ts, minval}
      var reply AccReply
      go func(pid int) {
        if pid == px.me {
          px.Accept(&args, &reply)
        } else {
          call(px.peers[pid], "Paxos.Accept", &args, &reply)
        }
        accResp <- reply
      }(pid)
    }
    // collect majority Accept responses
    respNum = 0
    delayNum = 0
    done = false
    for !done {
      select {
        case reply := <-accResp:
          if reply.Ok && reply.Ts == ts {
            respNum ++
            if respNum > len(px.peers) / 2 {
              done = true
            }
          }
        default:
         time.Sleep(DelayInterval)
         delayNum ++
         if delayNum >= DelayMaxNum {
           done = true
         }
      }
    }
    if respNum <= len(px.peers) / 2 {
      time.Sleep( time.Duration(rand.Int31() % 1000) * time.Millisecond )
      continue
    }
    // The proposal has been accepted by majority. Send decided to all
    for pid := 0; pid < len(px.peers); pid++ {
      args := DecArgs{seq, minval, px.me, px.minseq[px.me]}
      var reply DecReply 
      if pid == px.me {
        px.Decide(&args, &reply)
      } else {
        go func(pid int) {
          if pid == px.me {
            px.Decide(&args, &reply)
          } else {
            call(px.peers[pid], "Paxos.Decide", &args, &reply)
          }
        }(pid)
      }
    }
  }
}

func (px *Paxos) getInstance(seq int) *Instance {
  if seq < px.Min() {
    return nil
  }
  _, ok := px.instances[seq]
  if !ok && px.persistent {
    filename := "./paxos_log/"+px.name+"_INS"+strconv.Itoa(seq)+".txt"
    if _, err := os.Stat(filename); err == nil {
      numFile, err := os.Open(filename)
      if err != nil {
        log.Fatal(err)
      }
      px.instances[seq] = new(Instance)
      dec:= gob.NewDecoder(numFile)
      err = dec.Decode(px.instances[seq])
      numFile.Close()
      ok = true
    }
  }
  if !ok {
    px.instances[seq] = new(Instance)
    if px.instances[seq] == nil {
      log.Fatal("[getInstance] New failure");
    }
    px.instances[seq].Initialize()
  }
  return px.instances[seq]
}

func (px *Paxos) Prepare(args *PreArgs, reply *PreReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.getInstance(args.Seq)  
  if ins != nil && args.Ts > ins.N_p {
    ins.N_p = args.Ts
    reply.Ok = true
    reply.Ts = ins.N_a
    reply.Value = ins.V_a
    if px.persistent {
      filename := "./paxos_log/"+px.name+"_INS"+strconv.Itoa(args.Seq)+".txt"
      numFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
      if err != nil {
        log.Fatal(err)
      }
      buf := new(bytes.Buffer)
      enc := gob.NewEncoder(buf)
      enc.Encode(ins)
      numFile.Write(buf.Bytes())
      numFile.Close()
    }
  } else {
    reply.Ok = false
  }

  return nil
}

func (px *Paxos) Accept(args *AccArgs, reply *AccReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  ins := px.getInstance(args.Seq)
  if ins != nil && args.Ts >= ins.N_p {
    ins.N_p = args.Ts
    ins.N_a = args.Ts
    ins.V_a = args.Value
    reply.Ok = true
    reply.Ts = args.Ts
    if px.persistent {
      filename := "./paxos_log/"+px.name+"_INS"+strconv.Itoa(args.Seq)+".txt"
      numFile, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
      buf := new(bytes.Buffer)
      enc := gob.NewEncoder(buf)
      enc.Encode(ins)
      numFile.Write(buf.Bytes())
      numFile.Close()
    }
  } else {
    reply.Ok = false
    reply.Ts = args.Ts
  }
  return nil
}

func (px *Paxos) Decide(args *DecArgs, reply * DecReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
    
  ins := px.getInstance(args.Seq)
  if ins != nil {
    ins.Decided = true
    ins.V_a = args.Value
//    ins.Value = args.Value 
    if px.persistent {
      filename := "./paxos_log/"+px.name+"_INS"+strconv.Itoa(args.Seq)+".txt"
      numFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
      if err != nil {
        log.Fatal(err)
      }
      buf := new(bytes.Buffer)
      enc := gob.NewEncoder(buf)
      enc.Encode(ins)
      numFile.Write(buf.Bytes())
      numFile.Close()
    }
  }
  px.minseq[args.Me] = args.Doneseq
  px.Forget()
  return nil
}

func (px *Paxos) Forget() {
  min := px.Min()
  for k, _ := range px.instances {
    if k < min {
      if px.persistent {
        filename := "./paxos_log/"+px.name+"_INS"+strconv.Itoa(k)+".txt"
        if _, err := os.Stat(filename); os.IsNotExist(err) {
          log.Fatal("Log record does not exist!")
        }
        os.Remove(filename)
      }
      delete(px.instances, k) 
    }
  }
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  if seq > px.maxseq {
    px.maxseq = seq
  }
  if seq < px.minseq[px.me] {
    log.Fatal("ins = nil")
    return
  }
  go func() {
    px.Propose(seq, v)
  } ()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.minseq[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return px.maxseq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  min := 1 << 15 - 1
  for i := range px.minseq {
    if min > px.minseq[i] { 
      min = px.minseq[i]
    }
  }
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  ins, ok := px.instances[seq]
  if !ok {
    return false, nil
  } else {
    return ins.Decided, ins.V_a
  }
}

func (px *Paxos) RemoteStatus(args *PollArgs, reply *PollReply) error {
  reply.Ok, reply.Value = px.Status(args.Seq)
  return nil
}

// 
// Poll() is conceptually the same as Status(). 
// But Poll should ask all peers to make the 
// decision
//
func (px *Paxos) Poll(seq int) (bool, interface{}) {
  decided, value := px.Status(seq)
  if decided {
    return decided, value
  } else {
    // should ask peers for the status
    for pid := 0; pid < len(px.peers); pid++ {
      args := PollArgs{seq}
      var reply PollReply
      if pid != px.me {
        go func(pid int) {
          ok := call(px.peers[pid], "Paxos.RemoteStatus", &args, &reply)
          if ok && reply.Ok {
            px.mu.Lock()
            defer px.mu.Unlock()
            
            ins := px.getInstance(seq)
            ins.Decided = true
            ins.V_a = reply.Value
          }
        }(pid)
      }
    }
    time.Sleep(10 * time.Millisecond)
    return px.Status(seq)
  }
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

func (px *Paxos) Reboot() {
  px.dead = false
  os.Remove(px.peers[px.me]) // only needed for "unix"
  l, e := net.Listen("unix", px.peers[px.me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  px.l = l
  px.instances = make(map[int]*Instance)
  px.maxseq = 0
  px.minseq = make([]int, len(px.peers))
  for i := range px.minseq {
    px.minseq[i] = -1
  }
}

func MakePaxos(peers []string, me int, rpcs *rpc.Server, name string, persistent bool) *Paxos {
  px:= Make(peers, me, rpcs)
  px.name = name
  px.persistent = persistent
  return px
}
//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  os.Mkdir("paxos_log", 0666)
  px := &Paxos{}
  px.peers = peers
  px.me = me

  // Your initialization code here.
  px.instances = make(map[int]*Instance)
  px.maxseq = 0
  px.minseq = make([]int, len(peers))
  for i := range px.minseq {
    px.minseq[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
