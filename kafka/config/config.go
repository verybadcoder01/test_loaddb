package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka                string `yaml:"kafka"`
	KafkaTopic           string `yaml:"kafkaTopic"`
	KafkaPartition       int    `yaml:"kafkaPartition"`
	MaxThreads           int    `yaml:"maxThreads"`
	MaxMessagesPerThread int    `yaml:"maxMessagesPerThread"`
	MsgBatchSize         int    `yaml:"msgBatchSize"`
	DumpDir              string `yaml:"dumpDir"`
	MaxDumpSize          int    `yaml:"maxDumpSize"` // in megabytes
	MaxBufSize           int    `yaml:"maxBufSize"`
	MaxDeadThreads       int    `yaml:"maxDeadThreads"`
	MaxDeadTimeOut       int    `yaml:"maxDeadTimeOut"`
	WriterLogPath        string `yaml:"writerLogPath"`
	ReaderLogPath        string `yaml:"readerLogPath"`
	LogLevel             string `yaml:"logLevel"` // possible options are: trace, debug, info, warn, error, fatal, panic
}

func ParseConfig() Config {
	var conf Config
	file, err := os.ReadFile("./config.yaml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(file, &conf)
	if err != nil {
		log.Fatal("cant unmarshall config " + err.Error())
	}
	return conf
}
