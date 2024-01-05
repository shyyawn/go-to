package stores

import (
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type PostgresMock struct {
	mock.Mock
}

func (ds *PostgresMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}
