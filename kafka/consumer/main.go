package main

import (
	"context"
	"time"

	"dbload/config"
	"dbload/kafka/consumer/internal"
	"dbload/kafka/logger"
	tarantooldb "dbload/tarantool"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/tarantool/go-tarantool/v2"
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
	opts := tarantool.Opts{User: conf.Tarantool.User, Pass: conf.Tarantool.Password}
	db := tarantooldb.NewSpace(readerLogger, conf.Tarantool.Space, conf.Tarantool.Host, opts)
	closer.Bind(func() {
		if err := db.Close(); err != nil {
			readerLogger.Errorln(err)
		}
		if err := reader.Close(); err != nil {
			readerLogger.Errorln(err)
		}
		cancel()
		readerLogger.Infoln("finishing up")
	})
	readerLogger.SetReportCaller(true)
	db.SetupSpace()
	internal.StartConsuming(ctx, &db, conf.Performance.MaxThreads, conf.Tarantool.BatchSize, reader)
	closer.Close()
	closer.Hold()
}
