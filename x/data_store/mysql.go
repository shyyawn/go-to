package data_store

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"log"
	"sync"
	"time"
)

type Mysql struct {
	db                   *sql.DB
	User                 string `mapstructure:"user"`
	Password             string `mapstructure:"password"`
	Net                  string `mapstructure:"net"`
	Addr                 string `mapstructure:"addr"`
	DBName               string `mapstructure:"db_name"`
	AllowNativePasswords bool   `mapstructure:"allow_native_passwords"`
	Timeout              int    `json:"timeout"`
	ReadTimeout          int    `json:"read_timeout"`
	WriteTimeout         int    `json:"write_timeout"`
	MaxOpenConns         int    `json:"max_open_conns"`
	MaxIdleConns         int    `json:"max_idle_conns"`
	lock                 sync.RWMutex
}

func (ds *Mysql) LoadFromConfig(key string, config *viper.Viper) error {
	err := source.LoadFromConfig(key, config, ds)
	//Defaults
	if ds.Timeout == 0 {
		ds.Timeout = 5
	}
	if ds.ReadTimeout == 0 {
		ds.ReadTimeout = 60
	}
	if ds.WriteTimeout == 0 {
		ds.WriteTimeout = 60
	}
	if ds.MaxOpenConns == 0 {
		ds.MaxOpenConns = 5
	}
	if ds.MaxIdleConns == 0 {
		ds.MaxIdleConns = 5
	}
	return err
}

func (ds *Mysql) Db() *sql.DB {

	// if the db object is already created
	if ds.db != nil {
		// check ping
		if pingErr := ds.db.Ping(); pingErr == nil {
			return ds.db
		}
	}

	defer ds.lock.Unlock()
	ds.lock.Lock()
	cfg := mysql.Config{
		User:                 ds.User,
		Passwd:               ds.Password,
		Net:                  ds.Net,
		Addr:                 ds.Addr,
		DBName:               ds.DBName,
		Timeout:              time.Duration(ds.Timeout) * time.Second,
		ReadTimeout:          time.Duration(ds.ReadTimeout) * time.Second,
		WriteTimeout:         time.Duration(ds.WriteTimeout) * time.Second,
		AllowNativePasswords: ds.AllowNativePasswords,
	}

	var err error
	ds.db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatalln(err)
	}
	if ds.MaxOpenConns != 0 {
		ds.db.SetMaxOpenConns(ds.MaxOpenConns)
	}
	if ds.MaxIdleConns != 0 {
		ds.db.SetMaxIdleConns(ds.MaxIdleConns)
	}
	return ds.db
}
