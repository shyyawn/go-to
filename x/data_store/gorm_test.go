package data_store

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGorm_LoadFromConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]interface{}
		expected Gorm
	}{
		{
			name: "Basic config",
			config: map[string]interface{}{
				"gorm": map[string]interface{}{
					"driver":   "mysql",
					"user":     "dbuser",
					"password": "secret123",
					"addr":     "127.0.0.1:3306",
					"db_name":  "myapp",
				},
			},
			expected: Gorm{
				Driver:       "mysql",
				User:         "dbuser",
				Password:     "secret123",
				Addr:         "127.0.0.1:3306",
				DBName:       "myapp",
				Charset:      "utf8",
				Timeout:      5,
				ReadTimeout:  60,
				WriteTimeout: 60,
			},
		},
		{
			name: "Custom timeouts",
			config: map[string]interface{}{
				"gorm": map[string]interface{}{
					"driver":        "mysql",
					"user":          "root",
					"password":      "rootpass",
					"addr":          "db.example.com:3306",
					"db_name":       "production",
					"timeout":       10,
					"read_timeout":  30,
					"write_timeout": 45,
				},
			},
			expected: Gorm{
				Driver:       "mysql",
				User:         "root",
				Password:     "rootpass",
				Addr:         "db.example.com:3306",
				DBName:       "production",
				Charset:      "utf8",
				Timeout:      10,
				ReadTimeout:  30,
				WriteTimeout: 45,
			},
		},
		{
			name: "SQLite config",
			config: map[string]interface{}{
				"gorm": map[string]interface{}{
					"driver":        "sqlite",
					"db_name":       "test.db",
					"charset":       "utf8",
					"timeout":       5,
					"read_timeout":  60,
					"write_timeout": 60,
				},
			},
			expected: Gorm{
				Driver:       "sqlite",
				DBName:       "test.db",
				Charset:      "utf8",
				Timeout:      5,
				ReadTimeout:  60,
				WriteTimeout: 60,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := viper.New()
			cfg.MergeConfigMap(tc.config)

			gorm := &Gorm{}
			err := gorm.LoadFromConfig("gorm", cfg)

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, *gorm)
		})
	}
}

func TestGorm_Db_UnsupportedDriver(t *testing.T) {
	gorm := Gorm{
		Driver: "postgres", // Assuming postgres is not supported
	}

	db, err := gorm.Db()

	assert.Error(t, err)
	assert.Nil(t, db)
	assert.Contains(t, err.Error(), "unsupported database driver")
}
