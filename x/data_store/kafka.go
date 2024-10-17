package data_store

import (
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
)

type KafkaInterface interface {
	LoadFromConfig(string, *viper.Viper) error
	Producer() sarama.AsyncProducer
}
type Kafka struct {
	config          *sarama.Config
	lock            sync.RWMutex
	producer        sarama.AsyncProducer
	Hosts           []string `mapstructure:"hosts"`
	Topic           string   `mapstructure:"topic"`
	MaxMessageBytes int      `mapstructure:"max_message_bytes"`
	Username        string   `mapstructure:"username"`
	Password        string   `mapstructure:"password"`
	IsSASL          bool     `mapstructure:"is_sasl"`
}

func (ds *Kafka) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *Kafka) Producer() sarama.AsyncProducer {

	if ds.producer != nil {
		return ds.producer
	}

	defer ds.lock.Unlock()
	ds.lock.Lock()

	log.Info("Going to create the Kafka ASync Producer")
	ds.config = sarama.NewConfig()
	ds.config.Producer.RequiredAcks = sarama.WaitForAll
	ds.config.Producer.Retry.Max = 10
	ds.config.Producer.Return.Successes = false

	if ds.MaxMessageBytes == 0 {
		ds.MaxMessageBytes = 1000000
	}
	ds.config.Producer.MaxMessageBytes = ds.MaxMessageBytes

	var err error
	ds.producer, err = sarama.NewAsyncProducer(ds.Hosts, ds.config)
	if err != nil {
		log.Fatal("Failed to start Sarama producer:", err)
		ds.producer = nil
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range ds.producer.Errors() {
			log.Error("Failed to write access log entry, resetting producer:", err)
			ds.producer = nil
		}
	}()
	return ds.producer
}
