package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka struct {
		Brokers       []string `yaml:"brokers"`
		Topic         string   `yaml:"topic"`
		ConsumerGroup string   `yaml:"consumerGroup"`
	} `yaml:"kafka"`
	Producer struct {
		MsgBatchSize    int `yaml:"msgBatchSize"`
		WriteTimeOutSec int `yaml:"writeTimeOutSec"`
	} `yaml:"producer"`
	Database struct {
		DSN string `yaml:"DSN"`
	} `yaml:"database"`
	Performance struct {
		MaxThreads           int `yaml:"maxThreads"`
		MaxMessagesPerThread int `yaml:"maxMessagesPerThread"`
		MaxDeadThreads       int `yaml:"maxDeadThreads"`
		MaxDeadTimeOut       int `yaml:"maxDeadTimeOut"`
	} `yaml:"performance"`
	Consumer struct {
		ReadCommitIntervalSec int `yaml:"readCommitIntervalSec"`
		MinReadBytes          int `yaml:"minReadBytes"`
		MaxReadBytes          int `yaml:"maxReadBytes"`
	} `yaml:"consumer"`
	Dumps struct {
		DumpDir     string `yaml:"dumpDir"`
		MaxDumpSize int    `yaml:"maxDumpSize"` // in megabytes
		MaxBufSize  int    `yaml:"maxBufSize"`
	} `yaml:"dumps"`
	Logging struct {
		ProducerLogPath string `yaml:"producerLogPath"`
		ConsumerLogPath string `yaml:"consumerLogPath"`
		DbLogPath       string `yaml:"dbLogPath"`
		LogLevel        string `yaml:"logLevel"` // possible options are: trace, debug, info, warn, error, fatal, panic
	} `yaml:"logging"`
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
