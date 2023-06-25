package main

import (
	"context"
	"dbload/kafka/config"
	"dbload/kafka/producer/internal"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"time"
)

func measureTimeAndPrintData(start time.Time, conf config.Config, totalMessages int) {
	elapsed := time.Since(start)
	fmt.Printf("Writing %v messages in every thread using %v threads, so a total of %v messages took %s\n", conf.MaxMessagesPerThread, conf.MaxThreads, totalMessages, elapsed)
	fmt.Printf("So writing one message took about %v milliseconds\n", float32(elapsed.Milliseconds())/float32(totalMessages))
	fmt.Printf("System proccessed about %v messages per minute\n", float64(totalMessages)/elapsed.Minutes())
}

func main() {
	conf := config.ParseConfig()
	file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	// TODO: logging package with logrus + lumberjack
	log.SetOutput(file)
	conn, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka, conf.KafkaTopic, conf.KafkaPartition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	start := time.Now()
	defer measureTimeAndPrintData(start, conf, conf.MaxThreads*conf.MaxMessagesPerThread)
	internal.StartWriting(conn, conf)
}
