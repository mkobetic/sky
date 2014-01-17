package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactorCacheGetValue(t *testing.T) {
	c := newCache(2)
	c.add("foo0", 1)
	c.add("foo1", 2)
	c.add("foo2", 3)
	value, ok := c.getValue("foo0")
	assert.Equal(t, value, uint64(0))
	assert.Equal(t, ok, false)
	value, ok = c.getValue("foo1")
	assert.Equal(t, value, uint64(2))
	assert.Equal(t, ok, true)
	value, ok = c.getValue("foo2")
	assert.Equal(t, value, uint64(3))
	assert.Equal(t, ok, true)
}

func TestFactorCacheGetKey(t *testing.T) {
	c := newCache(2)
	c.add("foo0", 1)
	c.add("foo1", 2)
	c.add("foo2", 3)
	value, ok := c.getKey(1)
	assert.Equal(t, value, "")
	assert.Equal(t, ok, false)
	value, ok = c.getKey(2)
	assert.Equal(t, value, "foo1")
	assert.Equal(t, ok, true)
	value, ok = c.getKey(3)
	assert.Equal(t, value, "foo2")
	assert.Equal(t, ok, true)
}

func TestFactorCacheGetWithLRU(t *testing.T) {
	c := newCache(2)
	c.add("foo0", 1)
	c.add("foo1", 2)
	c.getValue("foo0")
	c.add("foo2", 3)
	_, ok := c.getValue("foo0")
	assert.Equal(t, ok, true)
	_, ok = c.getValue("foo1")
	assert.Equal(t, ok, false)
	_, ok = c.getValue("foo2")
	assert.Equal(t, ok, true)
}

func TestFactorCacheRemove(t *testing.T) {
	c := newCache(2)
	c.add("foo0", 1)
	c.add("foo1", 2)
	c.remove("foo0")
	_, ok := c.getValue("foo0")
	assert.Equal(t, ok, false)
	_, ok = c.getValue("foo1")
	assert.Equal(t, ok, true)
}
