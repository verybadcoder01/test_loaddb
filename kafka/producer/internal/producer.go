package internal

import (
	"context"
	"dbload/kafka/config"
	"dbload/kafka/producer/thread"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"math/rand"
	"sync"
	"time"
)

func GetMessage() string {
	return fmt.Sprintf("message %v", rand.Int())
}

const (
	ALLDEAD = iota
	ALLRESTART
	ALLOK
)

func writeToKafka(ctx context.Context, holder *thread.ThreadsHolder, conn *kafka.Conn, wg *sync.WaitGroup, maxMsg int, threadId int) {
	defer func() {
		holder.FinishThread(threadId)
	}()
	defer wg.Done()
	// will never time out
	err := conn.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Println(err)
	}
	for i := 0; i < maxMsg; i++ {
		msg := GetMessage()
		_, err = conn.WriteMessages(kafka.Message{Value: []byte(msg)})
		if err != nil {
			// кафка упала, надо об этом сказать
			log.Printf("error occured in thread %v\n", threadId)
			log.Println(err)
			// TODO: понять, какого черта 3 строки ниже кладут ВСЕ
			//holder.Mu.Lock()
			//holder.Threads[threadId].AppendBuffer(msg)
			//holder.Mu.Unlock()
			//holder[threadId].CriticalChan <- customErrors.NewCriticalError(err, threadId)
			holder.Threads[threadId].StatusChan <- thread.DEAD
		} else {
			holder.Threads[threadId].StatusChan <- thread.OK
		}
	}
	holder.Threads[threadId].StatusChan <- thread.FINISHED
	close(holder.Threads[threadId].StatusChan)
}

func StartWriting(conn *kafka.Conn, conf config.Config) {
	var wg sync.WaitGroup
	var holder thread.ThreadsHolder
	ctx, StopThreads := context.WithCancel(context.Background())
	for i := 0; i < conf.MaxThreads; i++ {
		c := make(chan error)
		s := make(chan thread.Status, 100)
		holder.Threads = append(holder.Threads, thread.Thread{IsDone: false, CriticalChan: c, MsgBuffer: []string{}, DumpPath: conf.DumpFile, StatusChan: s, MaxBufSize: conf.MaxBufSize})
	}
	for i := 0; i < conf.MaxThreads; i++ {
		wg.Add(1)
		go writeToKafka(ctx, &holder, conn, &wg, conf.MaxMessagesPerThread, i)
	}
	wg.Add(1)
	c := make(chan int)
	go StartArbitr(&wg, conf, &holder, c)
	log.Println("all goroutines are running")
	res := <-c
	if res == ALLDEAD {
		log.Fatalf("system dead.")
	} else if res == ALLRESTART {
		//TODO: restart all
	} else {
		log.Println("all good, all messages sent!")
	}
	StopThreads()
	wg.Wait()
}

func StartArbitr(wg *sync.WaitGroup, conf config.Config, holder *thread.ThreadsHolder, resChan chan int) {
	defer wg.Done()
	deadCnt := 0
outer:
	for {
		if holder.CountType(thread.FINISHED) == len(holder.Threads) {
			log.Println("finish arbitr")
			resChan <- ALLOK
			return
		}
		deadCnt = holder.CountType(thread.DEAD)
		if deadCnt > conf.MaxDeadThreads {
			break outer
		}
	}
	log.Println("starting retry")
	retrySuccessfull := false
	ticker := time.NewTicker(1 * time.Second)
	attempts := 0
	for range ticker.C {
		log.Printf("retry attempt %v\n", attempts)
		newConn, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka, conf.KafkaTopic, conf.KafkaPartition)
		_, err = newConn.ReadPartitions()
		if err != nil {
			log.Println("fail!")
			retrySuccessfull = false
		} else {
			log.Println("success!")
			retrySuccessfull = true
			ticker.Stop()
			break
		}
		attempts++
		if attempts > conf.MaxDeadTimeOut {
			break
		}
	}
	log.Printf("retry success is %v\n", retrySuccessfull)
	if !retrySuccessfull {
		log.Println("retry failed, killing all threads")
		resChan <- ALLDEAD
		return
	} else {
		log.Println("retry ok, restarting everything")
		resChan <- ALLRESTART
		return
	}
}
