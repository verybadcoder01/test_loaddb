package logger

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LoggerConfig struct {
	FileName  string
	LogLevel  log.Level
	Formatter log.Formatter
}

func NewLoggerConfig(level string, file string, formatter log.Formatter) LoggerConfig {
	var res LoggerConfig
	switch level {
	case "trace":
		res.LogLevel = log.TraceLevel
	case "debug":
		res.LogLevel = log.DebugLevel
	case "info":
		res.LogLevel = log.InfoLevel
	case "warn":
		res.LogLevel = log.WarnLevel
	case "error":
		res.LogLevel = log.ErrorLevel
	case "fatal":
		res.LogLevel = log.FatalLevel
	case "panic":
		res.LogLevel = log.PanicLevel
	default:
		panic("unknown logging level. Check the config!")
	}
	res.FileName = file
	res.Formatter = formatter
	return res
}

func SetupLogging(conf LoggerConfig, logger *log.Logger) {
	logger.SetOutput(&lumberjack.Logger{
		Filename:   conf.FileName,
		MaxSize:    32,
		MaxBackups: 2,
		MaxAge:     28,
		Compress:   true,
	})
	logger.SetLevel(conf.LogLevel)
	logger.SetFormatter(conf.Formatter)
}
