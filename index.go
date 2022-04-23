package cache_redis

import (
	"github.com/chefsgo/cache"
)

func Driver() cache.Driver {
	return &redisDriver{}
}

func init() {
	cache.Register("redis", Driver())
}
