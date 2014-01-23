package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/skydb/sky/core"
)

var defaultShardCount = runtime.NumCPU()

// func init() { defaultShardCount = 1 }

// DB represents access to the low-level data store.
type DB interface {
	Open() error
	Close()
	Factorizer() *Factorizer
	Cursors(tablespace string) (Cursors, error)
	GetEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error)
	GetEvents(tablespace string, id string) ([]*core.Event, error)
	InsertEvent(tablespace string, id string, event *core.Event) error
	InsertEvents(tablespace string, id string, newEvents []*core.Event) error
	InsertObjects(tablespace string, objects map[string][]*core.Event) (int, error)
	DeleteEvent(tablespace string, id string, timestamp time.Time) error
	DeleteObject(tablespace string, id string) error
	Merge(tablespace string, destinationId string, sourceId string) error
	Drop(tablespace string) error
}

// db is the default implementation of the DB interface.
type db struct {
	sync.RWMutex
	factorizer *Factorizer
	path       string
	shards     []*shard
	noSync     bool
	maxDBs     uint
	maxReaders uint
}

// Creates a new DB instance with data storage at the given path.
func New(path string, noSync bool, maxDBs uint, maxReaders uint) DB {
	f := NewFactorizer(filepath.Join(path, "factors"))
	f.NoSync = noSync
	f.MaxDBs = maxDBs
	f.MaxReaders = maxReaders

	return &db{
		factorizer: f,
		path:       path,
		noSync:     noSync,
		maxDBs:     maxDBs,
		maxReaders: maxReaders,
	}
}

func (db *db) dataPath() string {
	return filepath.Join(db.path, "data")
}

func (db *db) shardPath(index int) string {
	return filepath.Join(db.dataPath(), strconv.Itoa(index))
}

// Opens the database.
func (db *db) Open() error {
	db.Lock()
	defer db.Unlock()

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(db.dataPath(), 0700); err != nil {
		return err
	}

	// Open factorizer.
	if err := db.factorizer.Open(); err != nil {
		return err
	}

	// Determine shard count.
	shardCount, err := db.shardCount()
	if err != nil {
		return err
	}

	// Create and open each shard.
	db.shards = make([]*shard, 0)
	for i := 0; i < shardCount; i++ {
		db.shards = append(db.shards, newShard(db.shardPath(i)))
		if err := db.shards[i].Open(db.maxDBs, db.maxReaders, options(db.noSync)); err != nil {
			db.close()
			return err
		}
	}

	return nil
}

// Close shuts down all open database resources.
func (db *db) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
	db.factorizer.Close()
}

func (db *db) close() {
	for _, s := range db.shards {
		s.Close()
	}
	db.shards = nil
}

// getShardByObjectId retrieves the appropriate shard for a given object identifier.
func (db *db) getShardByObjectId(id string) *shard {
	index := core.ObjectLocalHash(id) % uint32(len(db.shards))
	return db.shards[index]
}

// shardCount retrieves the number of shards in the database. This is determined
// by the number of numeric directories in the data path. If no directories exist
// then a default count is used.
func (db *db) shardCount() (int, error) {
	infos, err := ioutil.ReadDir(db.dataPath())
	if err != nil {
		return 0, err
	}

	count := 0
	for _, info := range infos {
		index, err := strconv.Atoi(info.Name())
		if info.IsDir() && err == nil && (index+1) > count {
			count = index + 1
		}
	}

	if count == 0 {
		count = defaultShardCount
	}

	return count, nil
}

// LockAll obtains locks on the database and all shards.
func (db *db) LockAll() {
	db.Lock()
	for _, s := range db.shards {
		s.Lock()
	}
}

// UnlockAll releases locks on the database and all shards.
// Only use this with LockAll().
func (db *db) UnlockAll() {
	db.Unlock()
	for _, s := range db.shards {
		s.Unlock()
	}
}

// Factorizer returns the database's factorizer.
func (db *db) Factorizer() *Factorizer {
	return db.factorizer
}

// Cursors retrieves a set of cursors for iterating over the database.
func (db *db) Cursors(tablespace string) (Cursors, error) {
	cursors := make(Cursors, 0)
	for _, s := range db.shards {
		c, err := s.Cursor(tablespace)
		if err != nil {
			cursors.Close()
			return nil, fmt.Errorf("db cursors error: %s", err)
		}
		cursors = append(cursors, c)
	}
	return cursors, nil
}

func (db *db) GetEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error) {
	s := db.getShardByObjectId(id)
	return s.GetEvent(tablespace, id, timestamp)
}

func (db *db) GetEvents(tablespace string, id string) ([]*core.Event, error) {
	s := db.getShardByObjectId(id)
	return s.GetEvents(tablespace, id)
}

// InsertEvent adds a single event to the database.
func (db *db) InsertEvent(tablespace string, id string, event *core.Event) error {
	s := db.getShardByObjectId(id)
	return s.InsertEvent(tablespace, id, event)
}

// InsertEvents adds multiple events for a single object.
func (db *db) InsertEvents(tablespace string, id string, newEvents []*core.Event) error {
	s := db.getShardByObjectId(id)
	return s.InsertEvents(tablespace, id, newEvents)
}

// InsertObjects bulk inserts events for multiple objects.
func (db *db) InsertObjects(tablespace string, objects map[string][]*core.Event) (int, error) {
	count := 0
	for id, events := range objects {
		s := db.getShardByObjectId(id)
		if err := s.InsertEvents(tablespace, id, events); err != nil {
			return count, err
		}
		count += len(events)
	}
	return count, nil
}

func (db *db) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s := db.getShardByObjectId(id)
	return s.DeleteEvent(tablespace, id, timestamp)
}

func (db *db) DeleteObject(tablespace string, id string) error {
	s := db.getShardByObjectId(id)
	return s.DeleteObject(tablespace, id)
}

func (db *db) Merge(tablespace string, destinationId string, sourceId string) error {
	dest := db.getShardByObjectId(destinationId)
	src := db.getShardByObjectId(sourceId)

	// Retrieve source events.
	srcEvents, err := src.GetEvents(tablespace, sourceId)
	if err != nil {
		return err
	}

	// Insert events into destination object.
	if len(srcEvents) > 0 {
		if err = dest.InsertEvents(tablespace, destinationId, srcEvents); err != nil {
			return err
		}
		if err = src.DeleteObject(tablespace, sourceId); err != nil {
			return err
		}
	}

	return nil
}

// Drop removes a table from the database.
func (db *db) Drop(tablespace string) error {
	var err error
	for _, s := range db.shards {
		if _err := s.Drop(tablespace); err == nil {
			err = _err
		}
	}
	return err
}
