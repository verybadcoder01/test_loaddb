package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dbload/config"
	"dbload/kafka/logger"
	"dbload/kafka/message"
	"dbload/kafka/producer/buffer"
	"dbload/kafka/producer/internal"
	"dbload/kafka/producer/thread"
	"dbload/postgres/database"
	"dbload/postgres/filler"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/closer"
)

/*
	review:

Необходимо распечатать реальные измерения, тк возможны ощибки в коде, локи,... влияющие на скорость

Возможные метрики: min/avg/max rate, 50/90/100 quantile
*/
func measureTimeAndPrintData(start time.Time, conf config.Config, totalMessages int) {
	elapsed := time.Since(start)
	log.Infof("Writing %v messages in every thread using %v threads, so a total of %v messages took %s", conf.Performance.MaxMessagesPerThread, conf.Performance.MaxThreads, totalMessages, elapsed)
	log.Infof("So writing one message took about %v milliseconds", float32(elapsed.Milliseconds())/float32(totalMessages))
	log.Infof("System proccessed about %f messages per minute", float64(totalMessages)/elapsed.Minutes())
}

func main() {
	conf := config.ParseConfig()
	writerLogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.Logging.LogLevel, conf.Logging.ProducerLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), writerLogger)
	dblogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.Logging.LogLevel, conf.Logging.DBLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), dblogger)
	// just a way to ping kafka
	_, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka.Brokers[0], conf.Kafka.Topic, 0)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	var tmpMuList []*sync.Mutex
	var tmpThreadsList []thread.Thread
	for i := 0; i < conf.Performance.MaxThreads; i++ {
		s := make(chan thread.Status, 100)
		c := make(chan []message.Message, conf.Producer.MsgBatchSize+1)
		tmpMuList = append(tmpMuList, &sync.Mutex{})
		tmpThreadsList = append(tmpThreadsList,
			thread.NewThread(s, c,
				buffer.NewDequeBuffer(buffer.NewSimpleDumper(conf.Dumps.DumpDir+fmt.Sprintf("thread%v_dump.txt", i), int64(conf.Dumps.MaxDumpSize)),
					conf.Dumps.MaxBufSize,
				)))
	}
	holder := thread.NewThreadsHolder(tmpMuList, tmpThreadsList, writerLogger)
	ctx, cancel := context.WithCancel(context.Background())
	// db is thread-safe: https://stackoverflow.com/questions/62943920/what-is-the-best-way-to-use-gorm-in-multithreaded-application
	db := database.NewPgDatabase(conf.Database.DSN, conf.Database.CreateBatchSize, dblogger)
	dblogger.Infoln("successfully connected to database")
	// TODO: somehow move writer to main and put closing here
	closer.Bind(func() {
		// measureTimeAndPrintData(start, conf, conf.Performance.MaxThreads*conf.Performance.MaxMessagesPerThread)
		writerLogger.Infoln("finishing up")
		cancel()
	})
	db.InitTables()
	db.FillSupportTable(conf.Performance.MaxThreads, conf.Performance.MaxMessagesPerThread)
	go filler.Fill(ctx, db, conf.Database.CreateBatchSize)
	internal.StartWriting(ctx, db, writerLogger, conf, &holder)
	closer.Close()
	closer.Hold()
}
