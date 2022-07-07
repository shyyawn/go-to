package data_store

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

type RedisSentinel struct {
	MasterName    string   `mapstructure:"name"`
	SentinelAddrs []string `mapstructure:"addr"`
	Password      string   `mapstructure:"password"`
	client        *redis.Client
	ctx           context.Context
	lock          sync.RWMutex
}

func (ds *RedisSentinel) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *RedisSentinel) Client() *redis.Client {

	if ds.client != nil {
		return ds.client
	}

	defer ds.lock.Unlock()
	ds.lock.Lock()

	// Will see if need this for later use
	// Maybe can have it so that it has for health checks
	ds.ctx = context.Background()

	ds.client = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    ds.MasterName,
		SentinelAddrs: ds.SentinelAddrs,
		Password:      ds.Password,
	})

	pong, err := ds.client.Ping(ds.ctx).Result()
	if err != nil {
		log.Error(err)
		ds.client = nil
		return nil
	}

	log.Info(pong)
	return ds.client
}
