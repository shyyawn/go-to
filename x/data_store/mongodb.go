package data_store

import (
	"context"

	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
	DbName     string `mapstructure:"db_name"`
	Collection string `mapstructure:"collection"`
	UserName   string `mapstructure:"user_name"`
	Password   string `mapstructure:"password"`
	Host       string `mapstructure:"host"`
	Port       string `mapstructure:"port"`
	instance   *mongo.Client
}

func (ds *MongoDB) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *MongoDB) Instance() *mongo.Client {
	if ds.instance != nil {
		return ds.instance
	}
	credential := options.Credential{
		Username: ds.UserName,
		Password: ds.Password,
	}

	clientOptions := options.Client().ApplyURI("mongodb://" + ds.Host + ":" + ds.Port).SetAuth(credential)

	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func (ds *MongoDB) Dispose() {
	if ds.instance != nil {
		ds.instance.Disconnect(context.TODO())
		ds.instance = nil
	}
}
