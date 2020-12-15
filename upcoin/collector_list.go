package upcoin

import (
	"collector/db"
	"sync"
)

type (
	intervalSymbol struct {
		symbol   string
		interval string
	}

	collection struct {
		lastKline db.Kline
	}
)

type collections struct {
	m     map[intervalSymbol]*collection
	mutex sync.Mutex
}

func newCollections() *collections {
	list := &collections{
		m: make(map[intervalSymbol]*collection),
	}
	return list
}

func (c *collections) Get(key intervalSymbol) (*collection, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	v, exists := c.m[key]
	return v, exists
}

func (c *collections) Put(key intervalSymbol, val *collection)  {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.m[key] = val
}