package source

import "github.com/spf13/viper"

type Source interface {
	LoadFromConfig(key string, config *viper.Viper) error
}

func LoadFromConfig(key string, config *viper.Viper, source Source) error {
	if err := config.UnmarshalKey(key, source); err != nil {
		return err
	}
	return nil
}
