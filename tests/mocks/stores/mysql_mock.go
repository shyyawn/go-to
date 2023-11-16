package stores

import (
	"database/sql"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
)

type MysqlMock struct {
	mock.Mock
}

func (ds *MysqlMock) LoadFromConfig(key string, config *viper.Viper) error {
	args := ds.Called(key, config)
	return args.Error(0)
}

func (ds *MysqlMock) Db() *sql.DB {
	args := ds.Called()
	return args.Get(0).(*sql.DB)
}
