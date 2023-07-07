package main

import (
	"time"

	"dbload/kafka/config"
	"dbload/kafka/consumer/internal"
	"dbload/kafka/logger"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/closer"
)

/* review:
отсутвует обраюока ошибок
*/

func cleanup() {
	// TODO
	log.Println("finishing up")
}

func main() {
	conf := config.ParseConfig()
	readerLogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.LogLevel, conf.ReaderLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), readerLogger)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{conf.Kafka}, Topic: conf.KafkaTopic, StartOffset: kafka.FirstOffset,
		GroupID: conf.KafkaConsumerGroup,
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.RangeGroupBalancer{}, &kafka.RoundRobinGroupBalancer{}, &kafka.RackAffinityGroupBalancer{},
		},
		MaxBytes: conf.MaxReadBytes, MinBytes: conf.MinReadBytes,
		CommitInterval: time.Duration(conf.ReadCommitInterval) * time.Second,
	})
	closer.Bind(cleanup)
	internal.StartConsuming(conf.MaxThreads, readerLogger, reader)
	closer.Close()
	closer.Hold()
}
