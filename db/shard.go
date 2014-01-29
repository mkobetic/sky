package db

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/skydb/sky/core"
	"github.com/szferi/gomdb"
	"github.com/ugorji/go/codec"
)

// shard represents a subset of the database stored in a single LMDB environment.
type shard struct {
	sync.Mutex
	path string
	env  *mdb.Env
}

type Stat struct {
	Entries      uint64 `json:"entries"` // Number of data items
	Size         uint64 `json:"size"`    // Size of the data memory map
	Depth        uint   `json:"depth"`   // Depth (height) of the B-tree
	Transactions struct {
		Last uint64 `json:"last"` // ID of the last committed transaction
	} `json:"transactions"`
	Readers struct {
		Max     uint `json:"max"`     // maximum number of threads for the environment
		Current uint `json:"current"` // maximum number of threads used in the environment
	} `json:"readers"`
	Pages struct {
		Last     uint64 `json:"last"`     // ID of the last used page
		Size     uint   `json:"size"`     // Size of a database page. This is currently the same for all databases.
		Branch   uint64 `json:"branch"`   // Number of internal (non-leaf) pages
		Leaf     uint64 `json:"leaf"`     // Number of leaf pages
		Overflow uint64 `json:"overflow"` // Number of overflow pages
	} `json:"pages"`
}

// newShard creates a new shard.
func newShard(path string) *shard {
	return &shard{path: path}
}

func (s *shard) Stat() (*Stat, error) {
	stat, err := s.env.Stat()
	if err != nil {
		return nil, err
	}
	info, err := s.env.Info()
	if err != nil {
		return nil, err
	}
	ss := &Stat{
		Entries: stat.Entries,
		Size:    info.MapSize,
		Depth:   stat.Depth,
	}
	ss.Transactions.Last = info.LastTxnID
	ss.Readers.Max = info.MaxReaders
	ss.Readers.Current = info.NumReaders
	ss.Pages.Last = info.LastPNO
	ss.Pages.Size = stat.PSize
	ss.Pages.Branch = stat.BranchPages
	ss.Pages.Leaf = stat.LeafPages
	ss.Pages.Overflow = stat.OwerflowPages
	return ss, nil
}

// Open allocates a new LMDB environment.
func (s *shard) Open(maxDBs uint, maxReaders uint, options uint) error {
	s.Lock()
	defer s.Unlock()
	s.close()

	if err := os.MkdirAll(s.path, 0700); err != nil {
		return err
	}

	var err error
	s.env, err = mdb.NewEnv()
	if err != nil {
		return fmt.Errorf("lmdb env error: %s", err)
	}

	// LMDB environment settings.
	if err := s.env.SetMaxDBs(mdb.DBI(maxDBs)); err != nil {
		s.close()
		return fmt.Errorf("lmdb maxdbs error: %s", err)
	} else if err := s.env.SetMaxReaders(maxReaders); err != nil {
		s.close()
		return fmt.Errorf("lmdb maxreaders error: %s", err)
	} else if err := s.env.SetMapSize(2 << 40); err != nil {
		s.close()
		return fmt.Errorf("lmdb map size error: %s", err)
	}

	// Open the LMDB environment.
	if err := s.env.Open(s.path, options, 0664); err != nil {
		s.close()
		return fmt.Errorf("lmdb env open error: %s", err)
	}

	return nil
}

// Close releases all shard resources.
func (s *shard) Close() {
	s.Lock()
	defer s.Unlock()
	s.close()
}

func (s *shard) close() {
	if s.env != nil {
		s.env.Close()
		s.env = nil
	}
}

// cursor retrieves a cursor for iterating over the shard.
func (s *shard) cursor(tablespace string) (*mdb.Cursor, error) {
	txn, dbi, err := s.txn(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("shard cursor error: %s", err)
	}

	c, err := txn.CursorOpen(dbi)
	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf("lmdb cursor open error: %s", err)
	}

	return c, nil
}

// InsertEvent adds a single event to the shard.
func (s *shard) InsertEvent(tablespace string, id string, event *core.Event, replace bool) error {
	s.Lock()
	defer s.Unlock()
	return s.insertEvent(tablespace, id, event, replace)
}

func (s *shard) insertEvent(tablespace string, id string, event *core.Event, replace bool) error {
	return s.insertEvents(tablespace, id, []*core.Event{event}, replace)
}

// InsertEvents adds multiple events for a single object.
func (s *shard) InsertEvents(tablespace string, id string, newEvents []*core.Event, replace bool) error {
	s.Lock()
	defer s.Unlock()
	return s.insertEvents(tablespace, id, newEvents, replace)
}

func (s *shard) insertEvents(tablespace string, id string, newEvents []*core.Event, replace bool) error {
	if len(newEvents) == 0 {
		return nil
	}

	// Sort events in timestamp order.
	sort.Sort(core.EventList(newEvents))

	// Check the current state and perform an optimized append if possible.
	state, data, err := s.getState(tablespace, id)
	var appendOnly bool
	if state == nil {
		appendOnly = true
	} else {
		for _, newEvent := range newEvents {
			if !state.Timestamp.Before(newEvent.Timestamp) {
				appendOnly = false
				break
			}
		}
	}
	if appendOnly {
		return s.appendEvents(tablespace, id, newEvents, state, data)
	}

	// Retrieve the events and state for the object.
	events, state, err := s.getEvents(tablespace, id)
	if err != nil {
		return err
	}

	// Append all the new events and sort.
	for _, newEvent := range newEvents {
		// Merge events with an existing timestamp.
		merged := false
		for index, event := range events {
			if event.Timestamp.Equal(newEvent.Timestamp.Round(time.Microsecond)) {
				if replace {
					events[index] = newEvent
				} else {
					event.Merge(newEvent)
				}
				merged = true
				break
			}
		}

		// If no existing events exist then just append to the end.
		if !merged {
			events = append(events, newEvent)
		}
	}
	sort.Sort(core.EventList(events))

	// Deduplicate permanent state.
	state = &core.Event{Data: map[int64]interface{}{}}
	for _, event := range events {
		state.Timestamp = event.Timestamp
		event.DedupePermanent(state)
		state.MergePermanent(event)
	}

	// Remove all empty events.
	events = []*core.Event(core.EventList(events).NonEmptyEvents())

	// Write events back to the database.
	err = s.setEvents(tablespace, id, events, state)
	if err != nil {
		return err
	}

	return nil
}

// Writes a list of events for an object in table.
func (s *shard) setEvents(tablespace string, id string, events []*core.Event, state *core.Event) error {
	// Sort the events.
	sort.Sort(core.EventList(events))

	// Ensure state is correct before proceeding.
	if len(events) > 0 {
		if state != nil {
			state.Timestamp = events[len(events)-1].Timestamp
		} else {
			return errors.New("shard: missing state")
		}
	} else {
		state = nil
	}

	// Encode the events.
	buffer := new(bytes.Buffer)
	for _, event := range events {
		err := event.EncodeRaw(buffer)
		if err != nil {
			return err
		}
	}

	// Set the raw bytes.
	return s.write(tablespace, id, buffer.Bytes(), state)
}

// Appends events for a given object in a table to a servlet. This should not
// be called directly but only through PutEvent().
func (s *shard) appendEvents(tablespace string, id string, events []*core.Event, state *core.Event, data []byte) error {
	if state == nil {
		state = &core.Event{Data: map[int64]interface{}{}}
	}

	// Encode the event data to the end of the buffer.
	buffer := bytes.NewBuffer(data)
	for _, event := range events {
		state.Timestamp = event.Timestamp
		event.DedupePermanent(state)
		state.MergePermanent(event)

		// Append new event.
		if err := event.EncodeRaw(buffer); err != nil {
			return err
		}
	}

	// Write everything to the database.
	return s.write(tablespace, id, buffer.Bytes(), state)
}

// Writes a list of events for an object in tablespace.
func (s *shard) write(tablespace string, id string, data []byte, state *core.Event) error {
	var err error

	// Encode the state at the beginning.
	buffer := new(bytes.Buffer)
	var b []byte
	if state != nil {
		if b, err = state.MarshalRaw(); err != nil {
			return err
		}
	} else {
		b = []byte{}
	}
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewEncoder(buffer, &handle).Encode(b); err != nil {
		return err
	}

	// Encode the rest of the data.
	buffer.Write(data)

	// Begin a transaction.
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("lmdb txn begin error: %s", err)
	}
	defer txn.Commit()

	if err = txn.Put(dbi, []byte(id), buffer.Bytes(), mdb.NODUPDATA); err != nil {
		return fmt.Errorf("lmdb txn put error: %s", err)
	}

	return nil
}

// Retrieves an event for a given object at a single point in time.
func (s *shard) getEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error) {
	// Retrieve all events.
	events, _, err := s.getEvents(tablespace, id)
	if err != nil {
		return nil, err
	}

	// Find an event at a given point in time.
	for _, v := range events {
		if v.Timestamp.Equal(timestamp.Round(time.Microsecond)) {
			return v, nil
		}
	}

	return nil, nil
}

// Retrieves a list of events and the current state for a given object in a table.
func (s *shard) getEvents(tablespace string, id string) ([]*core.Event, *core.Event, error) {
	state, data, err := s.getState(tablespace, id)
	if err != nil {
		return nil, nil, err
	}

	events := make([]*core.Event, 0)
	if data != nil {
		reader := bytes.NewReader(data)
		for {
			event := &core.Event{}
			if err := event.DecodeRaw(reader); err == io.EOF {
				break
			} else if err != nil {
				return nil, nil, err
			}
			events = append(events, event)
		}
	}

	return events, state, nil
}

// Retrieves the state and the remaining serialized event stream for an object.
func (s *shard) getState(tablespace string, id string) (*core.Event, []byte, error) {
	// Begin a transaction.
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return nil, nil, fmt.Errorf("lmdb state error: %s", err)
	}

	// Retrieve byte array.
	data, err := txn.Get(dbi, []byte(id))
	txn.Commit()
	if err != nil && err != mdb.NotFound {
		return nil, nil, err
	}

	// Decode the events into a slice.
	if data != nil {
		reader := bytes.NewReader(data)

		// The first item should be the current state wrapped in a raw value.
		var raw interface{}
		var handle codec.MsgpackHandle
		handle.RawToString = true
		decoder := codec.NewDecoder(reader, &handle)
		if err := decoder.Decode(&raw); err != nil && err != io.EOF {
			return nil, nil, err
		}
		if b, ok := raw.(string); ok {
			state := &core.Event{}
			if err = state.DecodeRaw(bytes.NewReader([]byte(b))); err == nil {
				eventData, _ := ioutil.ReadAll(reader)
				return state, eventData, nil
			} else if err != io.EOF {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("shard: invalid state: %v", raw)
		}
	}

	return nil, []byte{}, nil
}

// deleteEvent removes a single event from the shard.
func (s *shard) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s.Lock()
	defer s.Unlock()

	// Retrieve the events for the object and append.
	tmp, _, err := s.getEvents(tablespace, id)
	if err != nil {
		return err
	}
	// Remove any event matching the timestamp.
	state := &core.Event{Data: map[int64]interface{}{}}
	events := make([]*core.Event, 0)
	for _, v := range tmp {
		if !v.Timestamp.Equal(timestamp) {
			events = append(events, v)
			state.MergePermanent(v)
		}
	}

	// Write events back to the database.
	err = s.setEvents(tablespace, id, events, state)
	if err != nil {
		return err
	}

	return nil
}

// Deletes all events for a given object in a table.
func (s *shard) DeleteEvents(tablespace, id string) error {
	s.Lock()
	defer s.Unlock()

	// Begin a transaction.
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("shard delete txn error: %s", err)
	}
	defer txn.Commit()

	// Delete the key.
	if err = txn.Del(dbi, []byte(id), nil); err != nil && err != mdb.NotFound {
		return fmt.Errorf("shard delete error: %s", err)
	}

	return nil
}

// Drop removes a table from the shard.
func (s *shard) Drop(tablespace string) error {
	s.Lock()
	defer s.Unlock()
	return s.drop(tablespace)
}

func (s *shard) drop(tablespace string) error {
	txn, dbi, err := s.txn(tablespace, false)
	if err != nil {
		return fmt.Errorf("drop txn error: %s", err)
	}
	defer txn.Commit()

	// Drop the table.
	if err = txn.Drop(dbi, 1); err != nil {
		return fmt.Errorf("drop error: %s", err)
	}

	return nil
}

func (s *shard) txn(name string, readOnly bool) (*mdb.Txn, mdb.DBI, error) {
	var flags uint = 0
	if readOnly {
		flags = flags | mdb.RDONLY
	}

	// Setup cursor to iterate over table.
	txn, err := s.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, 0, fmt.Errorf("skyd.Servlet: Unable to start LMDB transaction: %s", err)
	}
	var dbi mdb.DBI
	if readOnly {
		if dbi, err = txn.DBIOpen(&name, 0); err != nil && err != mdb.NotFound {
			return nil, 0, fmt.Errorf("Unable to open read-only LMDB DBI: %s", err)
		}
	} else {
		if dbi, err = txn.DBIOpen(&name, mdb.CREATE); err != nil {
			return nil, 0, fmt.Errorf("Unable to open writable LMDB DBI: %s", err)
		}
	}

	return txn, dbi, nil
}
