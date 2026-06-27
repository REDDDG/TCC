package repository

import (
	"github.com/redis/go-redis/v9"
)

var rdb *redis.Client

func Init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 10,
	})
}
