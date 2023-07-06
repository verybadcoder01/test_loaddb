package main

import (
	"time"

	"dbload/kafka/config"
	"dbload/kafka/consumer/internal"
	"dbload/kafka/logger"
	log "github.com/sirupsen/logrus"
)

/* review:
отсутвует обраюока ошибок
*/

func main() {
	conf := config.ParseConfig()
	readerLogger := log.New()
	logger.SetupLogging(logger.NewLoggerConfig(conf.LogLevel, conf.ReaderLogPath, &log.TextFormatter{
		PadLevelText: true, DisableColors: true, TimestampFormat: time.DateTime,
	}), readerLogger)
	internal.StartConsuming(&conf, readerLogger)
}
