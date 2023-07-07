package main

import (
	"context"
	"time"

	"dbload/kafka/config"
	"dbload/kafka/consumer/internal"
	"dbload/kafka/logger"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/closer"
)

func main() {
	conf := config.ParseConfig()
	readerLogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.Logging.LogLevel, conf.Logging.ConsumerLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), readerLogger)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: conf.Kafka.Brokers, Topic: conf.Kafka.Topic, StartOffset: kafka.FirstOffset,
		GroupID: conf.Kafka.ConsumerGroup,
		GroupBalancers: []kafka.GroupBalancer{
			&kafka.RangeGroupBalancer{}, &kafka.RoundRobinGroupBalancer{}, &kafka.RackAffinityGroupBalancer{},
		},
		MaxBytes: conf.Consumer.MaxReadBytes, MinBytes: conf.Consumer.MinReadBytes,
		CommitInterval: time.Duration(conf.Consumer.ReadCommitIntervalSec) * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	closer.Bind(func() {
		if err := reader.Close(); err != nil {
			readerLogger.Errorln(err)
		}
		cancel()
		readerLogger.Infoln("finishing up")
	})
	internal.StartConsuming(ctx, conf.Performance.MaxThreads, readerLogger, reader)
	closer.Close()
	closer.Hold()
}
