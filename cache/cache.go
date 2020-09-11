package cache

import (
	"github.com/gogf/gf/container/gtree"
	"github.com/gogf/gf/os/gtimer"
	"github.com/gogf/gf/util/gutil"
	"time"
)

type CachePool struct {
	DataPool *gtree.AVLTree
}

func (p *CachePool) InitCachePool() {
	p.DataPool = gtree.NewAVLTree(gutil.ComparatorString, true)
}

func (p *CachePool) Set(topic string, col string, key string, value map[string]interface{}, cacheTime int64) {
	if cacheTime == 0 {
		return
	}

	Key := topic + col + key
	p.DataPool.Set(Key, value)
	if cacheTime > 0 {
		interval := time.Duration(cacheTime) * time.Millisecond
		gtimer.Add(interval, func() {
			p.Remove(topic, col, key)
		})
	}
}

func (p *CachePool) Get(topic string, col string, key string) map[string]interface{} {
	Key := topic + col + key
	a := p.DataPool.Get(Key)
	if a != nil {
		return a.(map[string]interface{})
	} else {
		return nil
	}

}

func (p *CachePool) GetOnce(topic string, col string, key string) map[string]interface{} {
	Key := topic + col + key
	a := p.DataPool.Remove(Key)
	if a != nil {
		return a.(map[string]interface{})
	} else {
		return nil
	}
}

func (p *CachePool) Remove(topic string, col string, key string) {
	Key := topic + col + key
	p.DataPool.Remove(Key)
}
