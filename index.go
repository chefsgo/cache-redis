package cache_redis

import (
	"github.com/chefsgo/chef"
)

func Driver() chef.CacheDriver {
	return &redisCacheDriver{}
}

func init() {
	chef.Register("redis", Driver())
}
