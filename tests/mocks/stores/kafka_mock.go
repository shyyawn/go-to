package stores

import (
	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type KafkaMock struct {
	mock.Mock
}

func (ds *KafkaMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}

func (ds *KafkaMock) Producer() sarama.AsyncProducer {
	args := ds.Called()
	return args.Get(0).(sarama.AsyncProducer)
}
