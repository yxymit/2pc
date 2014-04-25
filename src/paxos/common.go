package paxos

import "time"

type PreArgs struct {
  Seq int
  Ts int64
}

type PreReply struct {
  Ok bool
  Ts int64
  Value interface{}
}

type AccArgs struct {
  Seq int
  Ts int64
  Value interface{}
}

type AccReply struct {
  Ok bool
  Ts int64
}

type DecArgs struct {
  Seq int
  Value interface{}
  Me int
  Doneseq int
}

type DecReply struct {
  Ok bool
}

type PollArgs struct {
  Seq int
}

type PollReply struct {
  Ok bool
  Value interface{}
}

const DelayInterval = time.Millisecond * 10
const DelayMaxNum = 10
