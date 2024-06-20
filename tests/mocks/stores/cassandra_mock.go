package stores

import (
	"github.com/gocql/gocql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type CassandraMock struct {
	mock.Mock
}

func (ds *CassandraMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}

func (ds *CassandraMock) Init() error {
	args := ds.Called()
	return args.Error(0)
}

func (ds *CassandraMock) Session() *gocql.Session {
	args := ds.Called()
	return args.Get(0).(*gocql.Session)
}

func (ds *CassandraMock) Cluster() *gocql.ClusterConfig {
	args := ds.Called()
	return args.Get(0).(*gocql.ClusterConfig)
}
