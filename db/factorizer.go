package db

import (
	"encoding/binary"
	"fmt"
	"github.com/skydb/sky/core"
	"github.com/szferi/gomdb"
	"os"
	"sync"
)

// maxKeySize is the size, in bytes, of the largest key that can be inserted.
// This is a limitation of LMDB.
const maxKeySize = 500

// cacheSize is the number of factors that are stored in the LRU cache.
// This cache size is per-property.
const cacheSize = 1000

// Factorizer manages the factorization and defactorization of values.
type Factorizer struct {
	sync.Mutex

	NoSync     bool
	MaxDBs     uint
	MaxReaders uint

	env    *mdb.Env
	path   string
	caches map[string]map[string]*cache
	txn    *mdb.Txn
	dirty  bool
}

// NewFactorizer returns a new Factorizer instance.
func NewFactorizer(path string) *Factorizer {
	return &Factorizer{
		path:       path,
		NoSync:     true,
		MaxDBs:     4096,
		MaxReaders: 126,
	}
}

// Path is the location of the factors database on disk.
func (f *Factorizer) Path() string {
	return f.path
}

// Open allocates a new LMDB environment.
func (f *Factorizer) Open() error {
	var err error

	f.Lock()
	defer f.Unlock()

	// Close the factorizer if it's already open.
	f.close()

	// Initialize and open a new environment.
	if err := os.MkdirAll(f.path, 0700); err != nil {
		return err
	}
	if f.env, err = mdb.NewEnv(); err != nil {
		return fmt.Errorf("factor env error: %s", err)
	}

	// LMDB environment settings.
	if err := f.env.SetMaxDBs(mdb.DBI(f.MaxDBs)); err != nil {
		f.close()
		return fmt.Errorf("factor maxdbs error: %s", err)
	} else if err := f.env.SetMaxReaders(f.MaxReaders); err != nil {
		f.close()
		return fmt.Errorf("factor maxreaders error: %s", err)
	} else if err := f.env.SetMapSize(2 << 40); err != nil {
		f.close()
		return fmt.Errorf("factor map size error: %s", err)
	}

	// Create LMDB flagset.
	var options uint = mdb.NOTLS
	if f.NoSync {
		options |= mdb.NOSYNC
	}

	// Open the LMDB environment.
	if err := f.env.Open(f.path, options, 0664); err != nil {
		f.close()
		return fmt.Errorf("factor env open error: %s", err)
	}

	// Open the writer.
	if err = f.renew(); err != nil {
		f.close()
		return fmt.Errorf("factor txn open error: %s", err)
	}

	// Initialize the cache.
	f.caches = make(map[string]map[string]*cache)

	return nil
}

// Close releases all factor resources.
func (f *Factorizer) Close() {
	f.Lock()
	defer f.Unlock()
	f.close()
}

func (f *Factorizer) close() {
	f.caches = nil

	if f.env != nil {
		f.env.Close()
		f.env = nil
	}
}

// Factorize converts a value for a property into a numeric identifier.
// If a value has already been factorized then it is reused. Otherwise a new
// one is created.
func (f *Factorizer) Factorize(tablespace string, id string, value string, createIfMissing bool) (uint64, error) {
	f.Lock()
	defer f.Unlock()
	defer f.renew()
	return f.factorize(tablespace, id, value, createIfMissing)
}

// Defactorize converts a previously factorized value from its numeric identifier
// to its string representation.
func (f *Factorizer) Defactorize(tablespace string, id string, value uint64) (string, error) {
	f.Lock()
	defer f.Unlock()
	defer f.renew()
	return f.defactorize(tablespace, id, value)
}

// FactorizeEvent converts all the values of an event into their numeric identifiers.
func (f *Factorizer) FactorizeEvent(event *core.Event, tablespace string, propertyFile *core.PropertyFile, createIfMissing bool) error {
	if event == nil {
		return nil
	}

	f.Lock()
	defer f.Unlock()
	defer f.renew()

	for k, v := range event.Data {
		property := propertyFile.GetProperty(k)
		if property.DataType == core.FactorDataType {
			if stringValue, ok := v.(string); ok {
				sequence, err := f.factorize(tablespace, property.Name, stringValue, createIfMissing)
				if err != nil {
					return err
				}
				event.Data[k] = sequence
			}
		}
	}

	return nil
}

// DefactorizeEvent converts all the values of an event from their numeric identifiers to their string values.
func (f *Factorizer) DefactorizeEvent(event *core.Event, tablespace string, propertyFile *core.PropertyFile) error {
	if event == nil {
		return nil
	}

	f.Lock()
	defer f.Unlock()
	defer f.renew()

	for k, v := range event.Data {
		property := propertyFile.GetProperty(k)
		if property.DataType == core.FactorDataType {
			var sequence uint64
			switch v := v.(type) {
			case int8:
				sequence = uint64(v)
			case int16:
				sequence = uint64(v)
			case int32:
				sequence = uint64(v)
			case int64:
				sequence = uint64(v)
			case uint8:
				sequence = uint64(v)
			case uint16:
				sequence = uint64(v)
			case uint32:
				sequence = uint64(v)
			case uint64:
				sequence = v
			case float32:
				sequence = uint64(v)
			case float64:
				sequence = uint64(v)
			}
			stringValue, err := f.defactorize(tablespace, property.Name, uint64(sequence))
			if err != nil {
				return err
			}
			event.Data[k] = stringValue
		}
	}

	return nil
}
func (f *Factorizer) factorize(tablespace string, id string, value string, createIfMissing bool) (uint64, error) {
	// Blank is always zero.
	if value == "" {
		return 0, nil
	}

	// Check the LRU first.
	c := f.cache(tablespace, id)
	if sequence, ok := c.getValue(value); ok {
		return sequence, nil
	}

	// Otherwise find it in the database.
	dbi, err := f.txn.DBIOpen(&tablespace, mdb.CREATE)
	if err != nil {
		return 0, fmt.Errorf("factor factorize dbi error: %s", err)
	}

	data, err := f.get(dbi, f.key(id, value))
	if err != nil {
		return 0, err
	} else if data != nil {
		return binary.BigEndian.Uint64(data), nil
	}

	// Create a new factor if requested.
	if createIfMissing {
		return f.add(dbi, tablespace, id, value)
	}

	err = NewFactorNotFound(fmt.Sprintf("skyd: Factor not found: %v", f.key(id, value)))
	return 0, err
}

// add creates a new factor for a given value.
func (f *Factorizer) add(dbi mdb.DBI, tablespace string, id string, value string) (uint64, error) {
	// Retrieve next id in sequence.
	sequence, err := f.nextid(dbi, id)
	if err != nil {
		return 0, err
	}

	// Truncate the value so it fits in our max key size.
	value = f.truncate(id, value)

	// Store the value-to-id lookup.
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], sequence)
	if err := f.put(dbi, f.key(id, value), data[:]); err != nil {
		return 0, err
	}

	// Save the id-to-value lookup.
	if err := f.put(dbi, f.revkey(id, sequence), []byte(value)); err != nil {
		return 0, err
	}

	// Add to cache.
	c := f.cache(tablespace, id)
	c.add(value, sequence)

	return sequence, nil
}

func (f *Factorizer) defactorize(tablespace string, id string, value uint64) (string, error) {
	// Blank is always zero.
	if value == 0 {
		return "", nil
	}

	// Check the cache first.
	c := f.cache(tablespace, id)
	if key, ok := c.getKey(value); ok {
		return key, nil
	}

	// Otherwise find it in the database.
	dbi, err := f.txn.DBIOpen(&tablespace, mdb.CREATE)
	if err != nil {
		return "", fmt.Errorf("factor defactorize dbi error: %s", err)
	}

	data, err := f.get(dbi, f.revkey(id, value))
	if err != nil {
		return "", err
	} else if data == nil {
		return "", fmt.Errorf("factor not found: %v", f.revkey(id, value))
	}

	// Add to cache.
	c.add(string(data), value)

	return string(data), nil
}

// Retrieves the next available sequence number within a table for an id.
func (f *Factorizer) nextid(dbi mdb.DBI, id string) (uint64, error) {
	seqkey := f.seqkey(id)
	data, err := f.get(dbi, seqkey)
	if err != nil {
		return 0, err
	}

	// Set sequence to zero if missing.
	var zero [8]byte
	if data == nil {
		data = zero[:]
	}

	// Read identifier and increment.
	seq := binary.BigEndian.Uint64(data)
	seq += 1

	// Save new sequence value.
	binary.BigEndian.PutUint64(data, seq)
	if err = f.put(dbi, seqkey, data); err != nil {
		return 0, err
	}
	return seq, nil
}

// get retrieves the value from the database for a given key.
func (f *Factorizer) get(dbi mdb.DBI, key string) ([]byte, error) {
	data, err := f.txn.Get(dbi, []byte(key))
	if err != nil && err != mdb.NotFound {
		return nil, fmt.Errorf("factor get error: %s", err)
	}
	return data, nil
}

// Sets the value for a given key in the database.
func (f *Factorizer) put(dbi mdb.DBI, key string, value []byte) error {
	if err := f.txn.Put(dbi, []byte(key), value, mdb.NODUPDATA); err != nil {
		return fmt.Errorf("factor put error: %s", err)
	}
	f.dirty = true
	return nil
}

// renew commits any dirty changes on the transaction and renews it.
func (f *Factorizer) renew() error {
	// Commit if dirty.
	if f.dirty {
		f.dirty = false
		if err := f.txn.Commit(); err != nil {
			return err
		}
		f.txn = nil
	}

	// Create a new transaction if needed.
	if f.txn == nil {
		var err error
		if f.txn, err = f.env.BeginTxn(nil, 0); err != nil {
			return fmt.Errorf("renew txn error: %s", err)
		}
	}

	return nil
}

// cache returns a reference to the LRU cache used for a given tablespace/id.
// If a cache doesn't exist then one will be created.
func (f *Factorizer) cache(tablespace string, id string) *cache {
	m := f.caches[tablespace]
	if m == nil {
		m = make(map[string]*cache)
		f.caches[tablespace] = m
	}
	c := m[id]
	if c == nil {
		c = newCache(cacheSize)
		m[id] = c
	}
	return c
}

// The key for a given id/value.
func (f *Factorizer) key(id string, value string) string {
	if s := fmt.Sprintf("%x:%s>%s", len(id), id, value); len(s) <= maxKeySize {
		return s
	}
	return fmt.Sprintf("%x:%s>%s", len(id), id, f.truncate(id, value))
}

// The reverse key for a given id/value.
func (f *Factorizer) revkey(id string, value uint64) string {
	return fmt.Sprintf("%x:%s<%d", len(id), id, value)
}

// The sequence key for a given id.
func (f *Factorizer) seqkey(id string) string {
	return fmt.Sprintf("%x:%s!", len(id), id)
}

// truncate returns the value that can be saved to the factorizer because of LMDB key size restrictions.
func (f *Factorizer) truncate(id string, value string) string {
	size := maxKeySize - len(fmt.Sprintf("%x:%s>", len(id), id))
	if size < len(value) {
		return value[0:size]
	}
	return value
}
