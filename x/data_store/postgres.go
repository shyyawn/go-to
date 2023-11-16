package data_store

import (
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

type PostgresInterface interface {
	LoadFromConfig(string, *viper.Viper) error
}

type Postgres struct {
}

func (ds *Postgres) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}
