package data_store

import (
	"net/http"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

const (
	ESDefaultTimeout = time.Second
)

type ElasticSearch struct {
	client  *elasticsearch.Client
	lock    sync.RWMutex
	Hosts   []string      `mapstructure:"hosts"`
	Timeout time.Duration `mapstructure:"timeout"`
	ApiKey  string        `mapstructure:"api_key"`
}

func (ds *ElasticSearch) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *ElasticSearch) Client() *elasticsearch.Client {

	if ds.client != nil {
		return ds.client
	}

	defer ds.lock.Unlock()
	ds.lock.Lock()

	var err error
	//cert, _ := ioutil.ReadFile(*cacert)
	if ds.Timeout == 0 {
		ds.Timeout = ESDefaultTimeout
	}
	cfg := elasticsearch.Config{
		Addresses: ds.Hosts,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: ds.Timeout,

			//TLSClientConfig: &tls.Config{
			//	MinVersion: tls.VersionTLS12,
			//	// ...
			//},
		},
		//Username: "",
		//Password: "",
		//CACert: cert,
		//CertificateFingerprint: fingerPrint,
	}

	if ds.ApiKey != "" {
		cfg.APIKey = ds.ApiKey
	}

	ds.client, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Error("Unable to connect to elastic search", err)
		ds.client = nil
	}

	return ds.client
}
