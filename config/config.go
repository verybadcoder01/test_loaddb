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
		MsgBatchSize         int  `yaml:"msgBatchSize"`
		SortedMsgBatchSize   int  `yaml:"sortedMsgBatchSize"`
		WriteTimeOutSec      int  `yaml:"writeTimeOutSec"`
		RequireTimestampSort bool `yaml:"requireTimestampSort"`
	} `yaml:"producer"`
	Database struct {
		DSN             string `yaml:"DSN"`
		CreateBatchSize int    `yaml:"createBatchSize"`
		CreateNewRows   bool   `yaml:"createNewRows"`
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
	Tarantool struct {
		User string `yaml:"user"`
		Host string `yaml:"host"`
	} `yaml:"tarantool"`
	Dumps struct {
		DumpDir     string `yaml:"dumpDir"`
		MaxDumpSize int    `yaml:"maxDumpSize"` // in megabytes
		MaxBufSize  int    `yaml:"maxBufSize"`
	} `yaml:"dumps"`
	Logging struct {
		ProducerLogPath string `yaml:"producerLogPath"`
		ConsumerLogPath string `yaml:"consumerLogPath"`
		DBLogPath       string `yaml:"dbLogPath"`
		LogLevel        string `yaml:"logLevel"` // possible options are: trace, debug, info, warn, error, fatal, panic
	} `yaml:"logging"`
}

func ValidateConfig(conf Config) {
	if conf.Producer.MsgBatchSize <= 0 {
		log.Fatal("Wrong value for message batch size: must be >0!")
	}
	if conf.Producer.WriteTimeOutSec <= 0 {
		log.Fatal("Wrong value for write timeout: must be >0 seconds!")
	}
	if conf.Database.CreateBatchSize <= 0 {
		log.Fatal("Wrong value for database creation batch size: must be >0!")
	}
	if conf.Performance.MaxThreads <= 0 || conf.Performance.MaxDeadThreads <= 0 || conf.Performance.MaxMessagesPerThread <= 0 || conf.Performance.MaxDeadTimeOut <= 0 {
		log.Fatal("All values in performance section must be >0!")
	}
	if conf.Performance.MaxMessagesPerThread%conf.Database.CreateBatchSize != 0 {
		log.Fatal("messages per thread must be divisible with creation batch size")
	}
	if conf.Consumer.ReadCommitIntervalSec <= 0 {
		log.Fatal("Wrong value for read commit interval: must be >0!")
	}
	if conf.Consumer.MinReadBytes > conf.Consumer.MaxReadBytes {
		log.Fatal("Minimal read bytes must be <= maximum read bytes!")
	}
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
	ValidateConfig(conf)
	return conf
}
