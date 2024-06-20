package stores

import (
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
	"gorm.io/gorm"
)

type GormMock struct {
	mock.Mock
}

func (ds *GormMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}

func (ds *GormMock) Db() *gorm.DB {
	args := ds.Called()
	return args.Get(0).(*gorm.DB)
}
