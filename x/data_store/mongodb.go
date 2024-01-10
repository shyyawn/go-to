package data_store

import (
	"context"
	"fmt"
	"strings"

	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
	DbName     string   `mapstructure:"db_name"`
	Collection string   `mapstructure:"collection"`
	UserName   string   `mapstructure:"user_name"`
	Password   string   `mapstructure:"password"`
	Host       string   `mapstructure:"host"`
	Replicas   []string `mapstructure:"replicas"`
	ReplicaSet string   `mapstructure:"replica_set"`
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

	clientOptions := options.Client().ApplyURI(ds.getConnectionString()).SetAuth(credential).SetReplicaSet(ds.ReplicaSet)

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

func (ds *MongoDB) getConnectionString() string {
	replicasString := strings.Join(ds.Replicas[:], ",")
	return fmt.Sprintf("mongodb://%s", replicasString)
}