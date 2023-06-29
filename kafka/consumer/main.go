package main

import (
	"context"
	"dbload/kafka/config"
	"dbload/kafka/logger"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	conf := config.ParseConfig()
	readerLogger := log.New()
	logger.SetupReaderLogging(conf, readerLogger)
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{conf.Kafka}, Topic: conf.KafkaTopic, StartOffset: kafka.FirstOffset, ReadBatchTimeout: 1 * time.Second, MaxAttempts: 1})
	for {
		if msg, err := reader.ReadMessage(context.Background()); err != nil {
			readerLogger.Errorln("Error reading Kafka:", err)
			break
		} else {
			readerLogger.Infof("topic=%s, partition=%d, offset=%d, key=%s, value=%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			time.Sleep(1 * time.Second)
		}
	}
}
