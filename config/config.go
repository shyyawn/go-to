package config

import (
	log "github.com/shyyawn/go-to/logging"
	"github.com/spf13/viper"
)

// This is the app configuration
var AppConfig = viper.New()

func Init(appPath string) {

	var err error
	AppConfig.AutomaticEnv()

	// If the ENV variable is set in environment, use that instead of passed environment
	env := AppConfig.GetString("ENV")
	if env == "" {
		env = "development"
	}
	AppConfig.SetConfigType("yaml")
	AppConfig.AddConfigPath(appPath + "/config/")

	// The main configuration file
	AppConfig.SetConfigName("main")
	if err = AppConfig.ReadInConfig(); err != nil {
		log.Fatal("Error on parsing configuration file", err)
	}
	log.Debug("Environment: ", env)
	// The environment configuration file
	AppConfig.SetConfigName(env)
	if err = AppConfig.MergeInConfig(); err != nil {
		log.Fatal("Error on parsing configuration file", err)
	}

	AppConfig.Set("runtime_dir", appPath)
}
