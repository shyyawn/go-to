package data_store

import (
	"log"

	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Gorm struct {
	User                 string `mapstructure:"user"`
	Password             string `mapstructure:"password"`
	Net                  string `mapstructure:"net"`
	Addr                 string `mapstructure:"addr"`
	DBName               string `mapstructure:"db_name"`
	Charset              string `mapstructure:"charset"`
	AllowNativePasswords bool   `mapstructure:"allow_native_passwords"`
	Debug                bool   `mapstructure:"debug"`
	Driver               string `mapstructure:"driver"`
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
	var dialector gorm.Dialector

	if ds.Driver == "" {
		ds.Driver = "mysql"
	}

	switch ds.Driver {
	case "mysql":
		dsn := ds.User + ":" + ds.Password + "@tcp(" + ds.Addr + ")/" + ds.DBName + "?charset=" + ds.Charset + "&parseTime=True&loc=Local"
		dialector = mysql.New(mysql.Config{
			DSN:                       dsn,   // data source name
			DefaultStringSize:         256,   // default size for string fields
			DisableDatetimePrecision:  true,  // disable datetime precision, which not supported before MySQL 5.6
			DontSupportRenameIndex:    true,  // drop & create when rename index, rename index not supported before MySQL 5.7, MariaDB
			DontSupportRenameColumn:   true,  // `change` when rename column, rename column not supported before MySQL 8, MariaDB
			SkipInitializeWithVersion: false, // auto configure based on currently MySQL version
		})
	case "sqlite":
		dialector = sqlite.Open(ds.DBName)
	default:
		log.Fatalf("Unsupported database driver: %s", ds.Driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if ds.Debug {
		db = db.Debug()
	}

	return db
}
