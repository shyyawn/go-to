package data_store

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

// RedisCluster @todo: Merge with redis
type RedisCluster struct {
	MasterName string   `mapstructure:"name"`
	Addr       []string `mapstructure:"addr"`
	//Username   string   `mapstructure:"username"`
	Password string `mapstructure:"password"`
	client   *redis.ClusterClient
	ctx      context.Context
	lock     sync.RWMutex
}

func (ds *RedisCluster) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *RedisCluster) Client() *redis.ClusterClient {

	if ds.client != nil {
		return ds.client
	}

	defer ds.lock.Unlock()
	ds.lock.Lock()

	// Will see if need this for later use
	// Maybe can have it so that it has for health checks
	ds.ctx = context.Background()

	ds.client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:      ds.Addr,
		Password:   ds.Password,
		MaxRetries: 3,
		PoolSize:   10,
	})

	err := ds.client.ForEachShard(ds.ctx, func(ctx context.Context, client *redis.Client) error {
		ping, err := client.Ping(ctx).Result()
		if err == nil {
			log.Info(ping)
		}
		return err
	})
	if err != nil {
		log.Error(err)
		ds.client = nil
		return nil
	}

	return ds.client
}
