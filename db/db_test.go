package db

import (
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/skydb/sky/core"
	"github.com/stretchr/testify/assert"
	"github.com/szferi/gomdb"
)

func TestDB(t *testing.T) {
	db := New("/tmp/sky", true, 1000, 100).(*db)
	assert.Equal(t, db.dataPath(), "/tmp/sky/data", "")
	assert.Equal(t, db.shardPath(2), "/tmp/sky/data/2", "")
	assert.Equal(t, int(db.maxDBs), 1000, "")
	assert.Equal(t, db.maxReaders, uint(100), "")
	assert.Equal(t, db.noSync, true, "")
}

func TestDBInsertEvent(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func TestDBInsertEvents(t *testing.T) {
	withDB(func(db *db) {
		input := []*core.Event{
			testevent("2000-01-01T00:00:02Z", 2, 100),
			testevent("2000-01-01T00:00:00Z", 1, "john"),
		}
		db.InsertEvents("foo", "bar", input, false)
		events, state, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 2, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "john", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-01T00:00:02Z"), "")
		assert.Equal(t, events[1].Data[2], 100, "")
		assert.Equal(t, state.Data[1], "john", "")
		assert.Equal(t, state.Data[2], 100, "")
	})
}

func TestDBInsertObjects(t *testing.T) {
	withDB(func(db *db) {
		input := map[string][]*core.Event{
			"bar": []*core.Event{
				testevent("2000-01-01T00:00:02Z", 2, 100),
				testevent("2000-01-01T00:00:00Z", 1, "john"),
			},
			"bat": []*core.Event{
				testevent("2000-01-01T00:00:00Z", 1, "jose"),
			},
		}

		n, err := db.InsertObjects("foo", input, false)
		assert.Nil(t, err, "")
		assert.Equal(t, n, 3, "")

		events, _, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 2, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "john", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-01T00:00:02Z"), "")
		assert.Equal(t, events[1].Data[2], 100, "")

		events, _, err = db.GetEvents("foo", "bat")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 1, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "jose", "")
	})
}

func TestDBInsertNonSequentialEvents(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-02T00:00:00Z", 1, "john", -1, 100), false)
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "jane", 2, "test"), false)
		db.InsertEvent("foo", "bar", testevent("2000-01-03T00:00:00Z", 1, "jose"), false)
		events, state, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 3, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[-1], nil, "")
		assert.Equal(t, events[0].Data[1], "jane", "")
		assert.Equal(t, events[0].Data[2], "test", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-02T00:00:00Z"), "")
		assert.Equal(t, events[1].Data[-1], 100, "")
		assert.Equal(t, events[1].Data[1], "john", "")
		assert.Equal(t, events[1].Data[2], nil, "")
		assert.Equal(t, events[2].Timestamp, musttime("2000-01-03T00:00:00Z"), "")
		assert.Equal(t, events[2].Data[-1], nil, "")
		assert.Equal(t, events[2].Data[1], "jose", "")
		assert.Equal(t, events[2].Data[2], nil, "")
		assert.Equal(t, state.Data[-1], nil, "")
		assert.Equal(t, state.Data[1], "jose", "")
		assert.Equal(t, state.Data[2], "test", "")
	})
}

func TestDBDeleteEvent(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		db.DeleteEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Nil(t, e, "")
	})
}

func TestDBDeleteMissingEvent(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		db.DeleteEvent("foo", "bar", musttime("2000-01-02T00:00:00Z"))
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.NotNil(t, e, "")
	})
}

func TestDBDeleteEvents(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		db.InsertEvent("foo", "bar", testevent("2000-01-02T00:00:00Z", 1, "jane"), false)
		db.DeleteEvents("foo", "bar")
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Nil(t, e, "")
	})
}

func TestDBMerge(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-03T00:00:00Z", 1, "john"), false)
		db.InsertEvent("foo", "bar", testevent("2000-01-02T00:00:00Z", 1, "jane"), false)
		db.InsertEvent("foo", "bat", testevent("2000-01-02T00:00:00Z", 1, "joe"), false)
		db.InsertEvent("foo", "bat", testevent("2000-01-01T00:00:00Z", 1, "jose"), false)
		err := db.Merge("foo", "bar", "bat")
		events, _, err := db.GetEvents("foo", "bar")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 3, "")
		assert.Equal(t, events[0].Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, events[0].Data[1], "jose", "")
		assert.Equal(t, events[1].Timestamp, musttime("2000-01-02T00:00:00Z"), "")
		assert.Equal(t, events[1].Data[1], "joe", "")
		assert.Equal(t, events[2].Timestamp, musttime("2000-01-03T00:00:00Z"), "")
		assert.Equal(t, events[2].Data[1], "john", "")
		events, _, err = db.GetEvents("foo", "bat")
		assert.Nil(t, err, "")
		assert.Equal(t, len(events), 0, "")
	})
}

func TestDBReopen(t *testing.T) {
	tmp := defaultShardCount
	defaultShardCount = 2
	withDB(func(db *db) {
		defaultShardCount = tmp

		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		db.Close()

		err := db.Open()
		assert.Nil(t, err, "")
		assert.Equal(t, len(db.shards), 2, "")

		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func TestDBCursors(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		db.InsertEvent("foo", "baz", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		cursors, err := db.Cursors("foo")
		defer cursors.Close()
		assert.Nil(t, err, "")

		keys := make([]string, 0)
		for _, c := range cursors {
			for {
				key, _, err := c.Get(nil, mdb.NEXT)
				if err == mdb.NotFound {
					break
				}
				assert.Nil(t, err, "")
				keys = append(keys, string(key))
			}
		}
		sort.Strings(keys)
		assert.Equal(t, keys, []string{"bar", "baz"}, "")
	})
}

func TestDBStats(t *testing.T) {
	withDB(func(db *db) {
		count, err := db.shardCount()
		assert.Nil(t, err, "")
		stats, err := db.Stats()
		assert.Nil(t, err, "")
		assert.Equal(t, len(stats), count)
	})
}

func TestDBDrop(t *testing.T) {
	withDB(func(db *db) {
		db.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		db.Drop("foo")
		e, err := db.GetEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Nil(t, e, "")
	})
}

func withDB(f func(db *db)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	db := New(path, false, 4096, 126).(*db)
	if err := db.Open(); err != nil {
		panic(err.Error())
	}
	defer db.Close()

	f(db)
}

func testevent(timestamp string, args ...interface{}) *core.Event {
	e := &core.Event{Timestamp: musttime(timestamp)}
	e.Data = make(map[int64]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].(int)
		e.Data[int64(key)] = args[i+1]
	}
	return e
}

func musttime(timestamp string) time.Time {
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		panic(err)
	}
	return t
}
