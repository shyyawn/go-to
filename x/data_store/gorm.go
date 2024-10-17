package data_store

import (
	"fmt"
	"time"

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
	Timeout              int    `mapstructure:"timeout"`
	ReadTimeout          int    `mapstructure:"read_timeout"`
	WriteTimeout         int    `mapstructure:"write_timeout"`
}

func (ds *Gorm) setDefaults() {
	if ds.Driver == "" {
		ds.Driver = "mysql"
	}
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
}

func (ds *Gorm) LoadFromConfig(key string, config *viper.Viper) error {
	err := source.LoadFromConfig(key, config, ds)
	if err != nil {
		return err
	}
	ds.setDefaults()
	return nil
}

func (ds *Gorm) Db() (*gorm.DB, error) {
	var dialector gorm.Dialector

	ds.setDefaults()

	switch ds.Driver {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&parseTime=True&loc=Local&timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
			ds.User, ds.Password, ds.Addr, ds.DBName, ds.Charset, ds.Timeout, ds.ReadTimeout, ds.WriteTimeout)
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
		return nil, fmt.Errorf("unsupported database driver: %s", ds.Driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	if ds.Debug {
		db = db.Debug()
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %v", err)
	}

	// Set connection pool settings
	sqlDB.SetConnMaxLifetime(time.Duration(ds.Timeout) * time.Second)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)

	return db, nil
}
