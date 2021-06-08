package config

import (
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/spf13/viper"
)

func New(appPath, configPath string) *viper.Viper {

	appConfig := viper.New()

	var err error
	appConfig.AutomaticEnv()

	// If the ENV variable is set in environment, use that instead of passed environment
	env := appConfig.GetString("ENV")
	if env == "" {
		env = "development"
	}
	appConfig.SetConfigType("yaml")
	appConfig.AddConfigPath(configPath + "/")

	// The main configuration file
	appConfig.SetConfigName("main")
	if err = appConfig.ReadInConfig(); err != nil {
		log.Fatal("Error on parsing configuration file", err)
	}
	log.Debug("Environment: ", env)
	// The environment configuration file
	appConfig.SetConfigName(env)
	if err = appConfig.MergeInConfig(); err != nil {
		log.Fatal("Error on parsing configuration file", err)
	}

	appConfig.Set("runtime_dir", appPath)

	return appConfig
}
