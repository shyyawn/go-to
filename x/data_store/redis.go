package data_store

import (
	"context"
	"github.com/go-redis/redis/v8"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

type Redis struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	client   *redis.Client
	ctx      context.Context
}

func (ds *Redis) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *Redis) Client() *redis.Client {

	if ds.client != nil {
		return ds.client
	}

	// Will see if need this for later use
	// Maybe can have it so that it has for health checks
	ds.ctx = context.Background()

	ds.client = redis.NewClient(&redis.Options{
		Addr:     ds.Addr,
		Password: ds.Password,
		DB:       ds.DB,
		//DialTimeout:  time.Second,
		//MinIdleConns: 10,
		//MaxRetries:   10,
		//ReadTimeout:  time.Second * 60,
		//IdleTimeout:  time.Second * (60 * 120),
	})

	pong, err := ds.client.Ping(ds.ctx).Result()
	if err != nil {
		log.Error(err)
		return nil
	}

	log.Info(pong)
	return ds.client
}
