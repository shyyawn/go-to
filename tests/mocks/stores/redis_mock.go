package stores

import (
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type RedisMock struct {
	mock.Mock
}

func (ds *RedisMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}

func (ds *RedisMock) Client() *redis.Client {
	args := ds.Called()
	return args.Get(0).(*redis.Client)
}
