package db

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardGetEventFromEmptyEnv(t *testing.T) {
	withShard(func(s *shard) {
		e, err := s.GetEvent("tbl0", "obj0", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, e, "")
		assert.Nil(t, err, "")
	})
}

func TestShardInsertEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "john"))
		e, err := s.GetEvent("tbl0", "obj0", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func TestShardMergeEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "aaa", 2, 100))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-03T00:00:00Z", 1, "bbb", 2, 200))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "ccc", 2, 300))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "ddd"))
		e, err := s.GetEvent("tbl0", "obj0", musttime("2000-01-02T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-02T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "ddd", "")
		assert.Equal(t, e.Data[2], 300, "")
	})
}

func TestShardGetMissingEventInExistingObject(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "john"))
		e, err := s.GetEvent("tbl0", "obj0", musttime("1990-01-01T00:00:00Z"))
		assert.Nil(t, e, "")
		assert.Nil(t, err, "")
	})
}

func TestShardGetMissingEventInMissingObject(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "john"))
		e, err := s.GetEvent("tbl0", "wrong_obj", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, e, "")
		assert.Nil(t, err, "")
	})
}

func TestShardGetEvents(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "xxx"))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "yyy"))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-03T00:00:00Z", 1, "zzz"))
		events, err := s.GetEvents("tbl0", "obj0")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 3)
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"))
		assert.Equal(t, events[0].Data[1], "yyy")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-02T00:00:00Z"))
		assert.Equal(t, events[1].Data[1], "xxx")
		assert.Equal(t, events[2].Timestamp, musttime("2000-01-03T00:00:00Z"))
		assert.Equal(t, events[2].Data[1], "zzz")
	})
}

func TestShardStat(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "xxx"))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "yyy"))
		s.InsertEvent("tbl0", "obj1", testevent("2000-01-03T00:00:00Z", 1, "zzz"))
		s.InsertEvent("tbl1", "obj0", testevent("2000-01-05T00:00:00Z", 1, "zzz"))
		s.InsertEvent("tbl1", "obj0", testevent("2000-01-06T00:00:00Z", 1, "zzz"))
		stat, err := s.Stat()
		assert.Nil(t, err, "")
		assert.Equal(t, stat.Entries, uint64(2))
		assert.Equal(t, stat.Size, uint64(0x20000000000))
		assert.Equal(t, stat.Depth, uint(1))
		assert.Equal(t, stat.Transactions.Last, uint64(5))
		assert.Equal(t, stat.Readers.Max, uint(126))
		assert.Equal(t, stat.Readers.Current, uint(0))
		assert.Equal(t, stat.Pages.Last, uint64(10))
		assert.Equal(t, stat.Pages.Size, uint(0x1000))
		assert.Equal(t, stat.Pages.Branch, uint64(0))
		assert.Equal(t, stat.Pages.Leaf, uint64(1))
		assert.Equal(t, stat.Pages.Overflow, uint64(0))
	})
}

func TestShardDeleteEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "xxx"))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "yyy"))
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-03T00:00:00Z", 1, "zzz"))
		err := s.DeleteEvent("tbl0", "obj0", musttime("2000-01-02T00:00:00Z"))
		assert.Nil(t, err, "")
		e, err := s.GetEvent("tbl0", "obj0", musttime("2000-01-02T00:00:00Z"))
		assert.Nil(t, e, "")
		assert.Nil(t, err, "")
		s.DeleteEvent("tbl0", "obj0", musttime("2000-01-01T00:00:00Z"))
		s.DeleteEvent("tbl0", "obj0", musttime("2000-01-03T00:00:00Z"))
		events, err := s.GetEvents("tbl0", "obj0")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 0, "")
	})
}

func withShard(f func(*shard)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	s := newShard(path)
	if err := s.Open(4096, 126, options(false)); err != nil {
		panic(err.Error())
	}
	defer s.Close()

	f(s)
}
