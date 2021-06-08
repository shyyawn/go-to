package gin_x

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

type HttpServer struct {
	Name        string
	Host        string
	Port        int
	Engine      *gin.Engine
	Environment string
}

// NewHttpServer returns an instance of
func NewHttpServer(name string) *HttpServer {
	return &HttpServer{
		Name:   name,
		Engine: gin.New(),
	}
}

// LoadFromConfig reads the conf for settings
func (server *HttpServer) LoadFromConfig(key string, config *viper.Viper) {
	server.Host = config.GetString(key + ".host")
	server.Port = config.GetInt(key + ".port")
	server.Environment = config.GetString("environment")

}
