package data_store

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"log"
	"time"
)

type Mysql struct {
	db       *sql.DB
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Net      string `mapstructure:"net"`
	Addr     string `mapstructure:"addr"`
	DBName   string `mapstructure:"db_name"`
}

func (ds *Mysql) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *Mysql) Db() *sql.DB {

	// if the db object is already created
	if ds.db != nil {
		// check ping
		if pingErr := ds.db.Ping(); pingErr == nil {
			return ds.db
		}
	}

	cfg := mysql.Config{
		User:         ds.User,
		Passwd:       ds.Password,
		Net:          ds.Net,
		Addr:         ds.Addr,
		DBName:       ds.DBName,
		Timeout:      5 * time.Second,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	var err error
	ds.db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatalln(err)
	}
	return ds.db
}
