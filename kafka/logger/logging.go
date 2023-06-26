package logger

import (
	"dbload/kafka/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"time"
)

func SetupLogging(conf config.Config) {
	log.SetOutput(&lumberjack.Logger{
		Filename:   conf.LogPath,
		MaxSize:    32, // megabytes
		MaxBackups: 2,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	})
	switch conf.LogLevel {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "panic":
		log.SetLevel(log.PanicLevel)
	default:
		panic("unknown logging level. Check the config!")
	}
	log.SetFormatter(&log.TextFormatter{
		PadLevelText:    true,
		DisableColors:   true,
		TimestampFormat: time.DateTime,
	})
}
