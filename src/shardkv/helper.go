package shardkv
import "strconv"

func intInSlice(a int, list []int) bool {
  for _, b := range list {
    if b == a {
      return true
    }
  }
  return false
}

func dirname(gid int64, sid int) string{
	return "gid_"+strconv.Itoa(int(gid))+"_sid_"+strconv.Itoa(sid)+"_data_dir/" 
}

