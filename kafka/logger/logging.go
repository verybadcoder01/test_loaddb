package logger

import (
	"time"

	"dbload/kafka/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

func SetupWriterLogging(conf config.Config, logger *log.Logger) {
	logger.SetOutput(&lumberjack.Logger{
		Filename:   conf.WriterLogPath,
		MaxSize:    32, // megabytes
		MaxBackups: 2,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	})
	switch conf.LogLevel {
	case "trace":
		logger.SetLevel(log.TraceLevel)
	case "debug":
		logger.SetLevel(log.DebugLevel)
	case "info":
		logger.SetLevel(log.InfoLevel)
	case "warn":
		logger.SetLevel(log.WarnLevel)
	case "error":
		logger.SetLevel(log.ErrorLevel)
	case "fatal":
		logger.SetLevel(log.FatalLevel)
	case "panic":
		logger.SetLevel(log.PanicLevel)
	default:
		panic("unknown logging level. Check the config!")
	}
	logger.SetFormatter(&log.TextFormatter{
		PadLevelText:    true,
		DisableColors:   true,
		TimestampFormat: time.DateTime,
	})
}

func SetupReaderLogging(conf config.Config, logger *log.Logger) {
	logger.SetOutput(&lumberjack.Logger{
		Filename:   conf.ReaderLogPath,
		MaxSize:    32,
		MaxBackups: 2,
		MaxAge:     28,
		Compress:   true,
	})
	switch conf.LogLevel {
	case "trace":
		logger.SetLevel(log.TraceLevel)
	case "debug":
		logger.SetLevel(log.DebugLevel)
	case "info":
		logger.SetLevel(log.InfoLevel)
	case "warn":
		logger.SetLevel(log.WarnLevel)
	case "error":
		logger.SetLevel(log.ErrorLevel)
	case "fatal":
		logger.SetLevel(log.FatalLevel)
	case "panic":
		logger.SetLevel(log.PanicLevel)
	default:
		panic("unknown logging level. Check the config!")
	}
	logger.SetFormatter(&log.TextFormatter{
		PadLevelText:    true,
		DisableColors:   true,
		TimestampFormat: time.DateTime,
	})
}
