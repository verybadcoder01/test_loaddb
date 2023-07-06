package main

import (
	"context"
	"time"

	"dbload/kafka/config"
	"dbload/kafka/logger"
	"dbload/kafka/producer/internal"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

/*
	review:

Необходимо распечатать реальные измерения, тк возможны ощибки в коде, локи,... влияющие на скорость

Возможные метрики: min/avg/max rate, 50/90/100 quantile
*/
func measureTimeAndPrintData(start time.Time, conf config.Config, totalMessages int) {
	elapsed := time.Since(start)
	log.Infof("Writing %v messages in every thread using %v threads, so a total of %v messages took %s", conf.MaxMessagesPerThread, conf.MaxThreads, totalMessages, elapsed)
	log.Infof("So writing one message took about %v milliseconds", float32(elapsed.Milliseconds())/float32(totalMessages))
	log.Infof("System proccessed about %f messages per minute", float64(totalMessages)/elapsed.Minutes())
}

/*
	review:

Хорошо бы в main создать все требуемые объекты и уже их передавать в друг другу и вспомогательным функциям
тогда:
 1. процесс смены одной библиотеки на другую будет проще + возможно использование нескольких библиотек в зависимости от настроек
 2. становится возможным unit-тестирование, так как возможно будет за'mock'ать нужное поведение

нет никакого graceful shutdown механизма
что будет, если прервать выполнение программы?
*/
func main() {
	conf := config.ParseConfig()
	writerLogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.LogLevel, conf.WriterLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), writerLogger)
	_, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka, conf.KafkaTopic, conf.KafkaPartition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	start := time.Now()
	defer measureTimeAndPrintData(start, conf, conf.MaxThreads*conf.MaxMessagesPerThread)
	internal.StartWriting(writerLogger, conf)
}
