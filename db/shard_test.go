package db

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardStat(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "xxx"), false)
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "yyy"), false)
		s.InsertEvent("tbl0", "obj1", testevent("2000-01-03T00:00:00Z", 1, "zzz"), false)
		s.InsertEvent("tbl1", "obj0", testevent("2000-01-05T00:00:00Z", 1, "zzz"), false)
		s.InsertEvent("tbl1", "obj0", testevent("2000-01-06T00:00:00Z", 1, "zzz"), false)
		stat, err := s.Stat()
		assert.Nil(t, err, "")
		assert.Equal(t, stat.Entries, uint64(2))
		assert.Equal(t, stat.Size, uint64(0x20000000000))
		assert.Equal(t, stat.Depth, uint(1))
		assert.Equal(t, stat.Transactions.Last, uint64(7))
		assert.Equal(t, stat.Readers.Max, uint(126))
		assert.Equal(t, stat.Readers.Current, uint(0))
		assert.Equal(t, stat.Pages.Last, uint64(10))
		assert.Equal(t, stat.Pages.Size, uint(0x1000))
		assert.Equal(t, stat.Pages.Branch, uint64(0))
		assert.Equal(t, stat.Pages.Leaf, uint64(1))
		assert.Equal(t, stat.Pages.Overflow, uint64(0))
	})
}

func withShard(f func(*shard)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	s := newShard(path)
	if err := s.Open(4096, 126, 0); err != nil {
		panic(err.Error())
	}
	defer s.Close()

	f(s)
}
