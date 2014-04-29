package shardkv

func intInSlice(a int, list []int) bool {
  for _, b := range list {
    if b == a {
      return true
    }
  }
  return false
}
