package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dbload/kafka/config"
	"dbload/kafka/logger"
	"dbload/kafka/producer/buffer"
	"dbload/kafka/producer/internal"
	"dbload/kafka/producer/thread"
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

нет никакого graceful shutdown механизма
что будет, если прервать выполнение программы?
*/
func main() {
	conf := config.ParseConfig()
	writerLogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.LogLevel, conf.WriterLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), writerLogger)
	// just a way to ping kafka
	_, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka, conf.KafkaTopic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	start := time.Now()
	var tmpMuList []*sync.Mutex
	var tmpThreadsList []thread.Thread
	for i := 0; i < conf.MaxThreads; i++ {
		s := make(chan thread.Status, 100)
		tmpMuList = append(tmpMuList, &sync.Mutex{})
		tmpThreadsList = append(tmpThreadsList,
			thread.NewThread(s,
				buffer.NewDequeBuffer(buffer.NewSimpleDumper(conf.DumpDir+fmt.Sprintf("thread%v_dump.txt", i), int64(conf.MaxDumpSize)),
					conf.MaxBufSize,
				)))
	}
	holder := thread.NewThreadsHolder(tmpMuList, tmpThreadsList, writerLogger)
	defer measureTimeAndPrintData(start, conf, conf.MaxThreads*conf.MaxMessagesPerThread)
	internal.StartWriting(writerLogger, conf, &holder)
}
