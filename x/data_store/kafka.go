package data_store

import (
	"github.com/Shopify/sarama"
	log "github.com/shyyawn/go-to/x/logging"
	"github.com/shyyawn/go-to/x/source"
	"github.com/spf13/viper"
	"sync"
)

type Kafka struct {
	config   *sarama.Config
	lock     *sync.RWMutex
	producer sarama.AsyncProducer
	Hosts    []string `mapstructure:"hosts"`
	Topic    string   `mapstructure:"topic"`
}

func (ds *Kafka) LoadFromConfig(key string, config *viper.Viper) error {
	return source.LoadFromConfig(key, config, ds)
}

func (ds *Kafka) Producer() sarama.AsyncProducer {
	ds.config = sarama.NewConfig()
	ds.config.Producer.RequiredAcks = sarama.WaitForAll
	ds.config.Producer.Retry.Max = 10
	ds.config.Producer.Return.Successes = false

	var err error
	ds.producer, err = sarama.NewAsyncProducer(ds.Hosts, ds.config)
	if err != nil {
		log.Fatal("Failed to start Sarama producer:", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range ds.producer.Errors() {
			log.Error("Failed to write access log entry:", err)
		}
	}()
	return ds.producer
}

func samnple() {

	//config := sarama.NewConfig()
	//config.Producer.Return.Successes = false
	//
	//producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	//if err != nil {
	//	gin_x.SendResponseError(ctx, 503, "Message delivery is down.")
	//}
	//
	//producer.Input() <- &sarama.ProducerMessage{
	//	Topic:     "",
	//	Key:       nil,
	//	Value:     nil,
	//	Headers:   nil,
	//	Metadata:  nil,
	//	Offset:    0,
	//	Partition: 0,
	//	Timestamp: time.Time{},
	//}
	//
	//select {
	//case err := <-producer.Errors():
	//	log.Warnf("Producer returned error: %v", err)
	//default:
	//	log.Info("Produced message default")
	//}
	//
	//producer.Input()
	//

}
