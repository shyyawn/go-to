package data_store

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"google.golang.org/api/option"
)

type Gcs struct {
	BaseUrl         string `mapstructure:"base_url"`
	BucketName      string `mapstructure:"bucket_name"`
	CredentialsFile string `mapstructure:"credentials_file"`
}

func (gcs *Gcs) LoadFromConfig(key string, config *viper.Viper) error {
	err := source.LoadFromConfig(key, config, gcs)

	// set CredentialsFile dir based on config_dir
	// this is best practice, because we have multiple main.go, in config we only specify
	// /keys/gc.json, but in the code we need to specify the full path or else it will
	// not work cause it will look for the file in the current directory
	gcs.CredentialsFile = config.GetString("config_dir") + gcs.CredentialsFile

	return err
}

// client
func (gcs *Gcs) Client() (*storage.Client, error) {
	return storage.NewClient(context.Background(), option.WithCredentialsFile(gcs.CredentialsFile))
}

// bucket
func (gcs *Gcs) Bucket() (*storage.BucketHandle, *storage.Client, error) {
	client, err := gcs.Client()
	if err != nil {
		return nil, nil, err
	}
	return client.Bucket(gcs.BucketName), client, nil
}
