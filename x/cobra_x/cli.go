package cobra_x

import (
	"github.com/spf13/viper"
)

type Cli struct {
	Name        string
	Environment string
}

func NewCli(name string) *Cli {
	return &Cli{
		Name: name,
	}
}

func (cli *Cli) LoadFromConfig(key string, config *viper.Viper) {
	cli.Environment = config.GetString("environment")
}
