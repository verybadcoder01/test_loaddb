package config

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

type Config struct {
	Kafka                string `yaml:"kafka"`
	KafkaTopic           string `yaml:"kafkaTopic"`
	KafkaPartition       int    `yaml:"kafkaPartition"`
	MaxThreads           int    `yaml:"maxThreads"`
	MaxMessagesPerThread int    `yaml:"maxMessagesPerThread"`
	DumpFile             string `yaml:"dumpFile"`
	MaxBufSize           int    `yaml:"maxBufSize"`
	MaxDeadThreads       int    `yaml:"maxDeadThreads"`
	MaxDeadTimeOut       int    `yaml:"maxDeadTimeOut"`
	LogPath              string `yaml:"logPath"`
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
