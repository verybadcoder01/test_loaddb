package main

import (
	"context"
	"dbload/kafka/config"
	"dbload/kafka/logger"
	"dbload/kafka/producer/internal"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func measureTimeAndPrintData(start time.Time, conf config.Config, totalMessages int) {
	elapsed := time.Since(start)
	log.Infof("Writing %v messages in every thread using %v threads, so a total of %v messages took %s", conf.MaxMessagesPerThread, conf.MaxThreads, totalMessages, elapsed)
	log.Infof("So writing one message took about %v milliseconds", float32(elapsed.Milliseconds())/float32(totalMessages))
	log.Infof("System proccessed about %f messages per minute", float64(totalMessages)/elapsed.Minutes())
}

func main() {
	conf := config.ParseConfig()
	logger.SetupLogging(conf)
	conn, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka, conf.KafkaTopic, conf.KafkaPartition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	start := time.Now()
	defer measureTimeAndPrintData(start, conf, conf.MaxThreads*conf.MaxMessagesPerThread)
	internal.StartWriting(conn, conf)
}
