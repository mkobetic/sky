package db

import "container/list"

// cache is an LRU cache that holds only the most recently accessed values.
type cache struct {
	maxEntries int
	ll         *list.List
	keymap     map[string]*list.Element
	valuemap   map[uint64]*list.Element
}

type entry struct {
	key   string
	value uint64
}

// newCache creates a new LRU Cache.
func newCache(maxEntries int) *cache {
	return &cache{
		maxEntries: maxEntries,
		ll:         list.New(),
		keymap:     make(map[string]*list.Element),
		valuemap:   make(map[uint64]*list.Element),
	}
}

// add adds a value for a given key to the cache.
func (c *cache) add(key string, value uint64) {
	if e, ok := c.keymap[key]; ok {
		c.ll.MoveToFront(e)
		e.Value.(*entry).value = value
		return
	}
	elem := c.ll.PushFront(&entry{key, value})
	c.keymap[key] = elem
	c.valuemap[value] = elem

	// Remove the oldest entry if we're above our threshold.
	if c.ll.Len() > c.maxEntries {
		elem := c.ll.Back()
		if elem != nil {
			entry := elem.Value.(*entry)
			c.ll.Remove(elem)
			delete(c.keymap, entry.key)
			delete(c.valuemap, entry.value)
		}
	}
}

// getByKey retrieves a value by key from the cache.
func (c *cache) getValue(key string) (uint64, bool) {
	if e, ok := c.keymap[key]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*entry).value, true
	}
	return 0, false
}

// getByValue retrieves a key by value from the cache.
func (c *cache) getKey(value uint64) (string, bool) {
	if e, ok := c.valuemap[value]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*entry).key, true
	}
	return "", false
}

// remove deletes the given key and its value from the cache.
func (c *cache) remove(key string) {
	if e, ok := c.keymap[key]; ok {
		c.ll.Remove(e)
		delete(c.keymap, e.Value.(*entry).key)
		delete(c.valuemap, e.Value.(*entry).value)
	}
}

// size returns the number of items in the cache.
func (c *cache) size() int {
	return c.ll.Len()
}
