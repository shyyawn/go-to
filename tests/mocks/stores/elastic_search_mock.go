package stores

import (
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type ElasticSearchMock struct {
	mock.Mock
}

func (ds *ElasticSearchMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}

func (ds *ElasticSearchMock) Client() *elasticsearch.Client {
	args := ds.Called()
	return args.Get(0).(*elasticsearch.Client)
}
