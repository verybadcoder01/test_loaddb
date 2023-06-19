package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

const (
	MaxMessagesPerThread = int64(2e5)
	MaxThread            = 8
)

func writeToKafka(conn *kafka.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	err := conn.SetWriteDeadline(time.Now().Add(10 * time.Minute))
	if err != nil {
		log.Println(err)
	}
	i := int64(0)
	for {
		_, err = conn.WriteMessages(kafka.Message{Value: []byte(fmt.Sprintf("message %v", i))})
		if err != nil {
			log.Println(err)
		}
		i++
		if i > MaxMessagesPerThread {
			break
		}
	}
}

func main() {
	topic := "bench-test"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}
	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < MaxThread; i++ {
		wg.Add(1)
		go writeToKafka(conn, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	totalMessages := MaxThread * MaxMessagesPerThread
	fmt.Printf("Writing %v messages in every thread using %v threads, so a total of %v messages took %s\n", MaxMessagesPerThread, MaxThread, totalMessages, elapsed)
	fmt.Printf("So writing one message took about %v milliseconds\n", float32(elapsed.Milliseconds())/float32(totalMessages))
	fmt.Printf("System proccessed about %v messages per minute\n", float64(totalMessages)/elapsed.Minutes())
}
