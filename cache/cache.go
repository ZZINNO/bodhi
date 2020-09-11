package cache

import (
	"github.com/gogf/gf/os/gcache"
	"time"
)

type CachePool struct {
	DataPool *gcache.Cache
}

func (p *CachePool) InitCachePool() {
	p.DataPool = gcache.New()
}

func (p *CachePool) Set(topic string, col string, key string, value map[string]interface{}, cacheTime int64) {
	if cacheTime == 0 {
		return
	}

	Key := topic + col + key
	p.DataPool.Set(Key, value, time.Duration(cacheTime)*time.Millisecond)
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
