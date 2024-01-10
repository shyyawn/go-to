package data_store

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

type Redis struct {
	Addr        string `mapstructure:"addr"`
	Password    string `mapstructure:"password"`
	DB          int    `mapstructure:"db"`
	ReadTimeout int    `mapstructure:"readtimeout"`
	Channel     string `mapstructure:"channel"`
	client      *redis.Client
	ctx         context.Context
	lock        sync.RWMutex
}

func (ds *Redis) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *Redis) Client() *redis.Client {

	if ds.client != nil {
		return ds.client
	}

	defer ds.lock.Unlock()
	ds.lock.Lock()

	// Will see if need this for later use
	// Maybe can have it so that it has for health checks
	ds.ctx = context.Background()

	ds.client = redis.NewClient(&redis.Options{
		Addr:        ds.Addr,
		Password:    ds.Password,
		DB:          ds.DB,
		ReadTimeout: time.Duration(ds.ReadTimeout),
		//DialTimeout:  time.Second,
		//MinIdleConns: 10,
		//MaxRetries:   10,
		//ReadTimeout:  time.Second * 60,
		//IdleTimeout:  time.Second * (60 * 120),
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
