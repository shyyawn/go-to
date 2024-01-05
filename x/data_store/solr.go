package data_store

import (
	"fmt"

	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"github.com/stevenferrer/solr-go"
)

type Solr struct {
	Scheme string `mapstructure:"scheme"`
	Host   string `mapstructure:"host"`
	Port   string `mapstructure:"port"`
	Client *solr.JSONClient
}

func (ds *Solr) LoadFromConfig(key string, config *viper.Viper) error {
	err := source.LoadFromConfig(key, config, ds)
	if err != nil {
		return err
	}

	ds.Client = solr.NewJSONClient(fmt.Sprintf("%s://%s:%s", ds.Scheme, ds.Host, ds.Port))

	return nil
}
