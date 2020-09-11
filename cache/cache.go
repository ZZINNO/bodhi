package cache

import "github.com/gogf/gf/container/gmap"

type CachePool struct {
	Data *gmap.Map
}

func (p *CachePool) initCachePool() {
	p.Data = gmap.New(true)
}

func (p *CachePool) Set(key string, col string, topic string, value map[string]interface{}) {
}

func (p *CachePool) Get(key string, col string, topic string) {}
