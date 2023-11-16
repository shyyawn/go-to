package data_store

import (
	"log"

	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type GormInterface interface {
	LoadFromConfig(string, *viper.Viper) error
	Db() *gorm.DB
}

type Gorm struct {
	User                 string `mapstructure:"user"`
	Password             string `mapstructure:"password"`
	Net                  string `mapstructure:"net"`
	Addr                 string `mapstructure:"addr"`
	DBName               string `mapstructure:"db_name"`
	Charset              string `mapstructure:"charset"`
	AllowNativePasswords bool   `mapstructure:"allow_native_passwords"`
	Timeout              int    `json:"timeout"`
	ReadTimeout          int    `json:"read_timeout"`
	WriteTimeout         int    `json:"write_timeout"`
}

func (ds *Gorm) LoadFromConfig(key string, config *viper.Viper) error {
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
	if ds.Charset == "" {
		ds.Charset = "utf8"
	}
	return err
}

func (ds *Gorm) Db() *gorm.DB {
	dsn := ds.User + ":" + ds.Password + "@tcp(" + ds.Addr + ")/" + ds.DBName + "?charset=" + ds.Charset + "&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       dsn,   // data source name
		DefaultStringSize:         256,   // default size for string fields
		DisableDatetimePrecision:  true,  // disable datetime precision, which not supported before MySQL 5.6
		DontSupportRenameIndex:    true,  // drop & create when rename index, rename index not supported before MySQL 5.7, MariaDB
		DontSupportRenameColumn:   true,  // `change` when rename column, rename column not supported before MySQL 8, MariaDB
		SkipInitializeWithVersion: false, // auto configure based on currently MySQL version
	}), &gorm.Config{})

	if err != nil {
		log.Fatalln(err)
	}

	return db
}
