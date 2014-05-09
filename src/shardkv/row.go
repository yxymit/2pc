package shardkv

import "log"

type Row struct {
  Value string
  Sharer []int // txn_ids sharing the row
  Lock_type string // 'share', 'exclusive', 'none'
}

func (row *Row) init() {
  row.Value = ""
  row.Sharer = make([]int, 0)
  row.Lock_type = "none"
}

func (row *Row) conflict(Type string) bool {
  if Type == "Get" {
    if row.Lock_type == "share" || row.Lock_type == "none" {
      return false
    } else {
      return true
    }
  } else { // Type == "Put" | "Add"
    if row.Lock_type == "none" {
      return false
    } else {
      return true
    }
  }
}

func (row *Row) lock(Type string, txnid int) {
  if row.conflict(Type) {
    log.Fatal("Lock Type Incompatible")
  }
  if Type == "Get" {
    row.Sharer = append(row.Sharer, txnid)
    row.Lock_type = "share"
  } else {
    if len(row.Sharer) > 0 {
      log.Fatal("Sharer exist!")
    }
    row.Sharer = append(row.Sharer, txnid) 
    row.Lock_type = "exclusive"
  }
}

func (row *Row) release(Type string, txnid int) {
  if Type == "Get" {
    index := -1
    for i, tid := range row.Sharer {
      if tid == txnid {
        index = i
      }
    }
    if index < 0 {
      DPrintfCLR(4, "sharer=%+v, txnid=%d", row.Sharer, txnid)
      log.Fatal("Not found in the Sharer list")
    }
    row.Sharer = append(row.Sharer[:index], row.Sharer[index+1:]...)
    if len(row.Sharer) == 0 {
      row.Lock_type = "none"
    }
  } else {
    if len(row.Sharer) != 1 {
      log.Fatal(len(row.Sharer), " txns exclusively locking the row!")
    }
    row.Sharer = make([]int, 0) 
    row.Lock_type = "none"
  }
}
