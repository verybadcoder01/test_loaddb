package internal

import (
	"context"
	"dbload/kafka/config"
	"dbload/kafka/producer/thread"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

func GetMessage() string {
	return fmt.Sprintf("message %v\n", rand.Int())
}

// arbitr statuses
const (
	ALLDEAD = iota
	ALLRESTART
	ALLOK
)

func writeToKafka(ctx context.Context, holder *thread.ThreadsHolder, writer *kafka.Writer, maxMsg int, batchSize int, threadId int) {
	defer holder.FinishThread(threadId)
	select {
	case <-ctx.Done():
		return
	default:
		for i := 0; i < maxMsg; {
			var batch []kafka.Message
			var data []string // in case we need dumping
			for j := 0; j < batchSize; j++ {
				msg := GetMessage()
				batch = append(batch, kafka.Message{Value: []byte(msg)})
				data = append(data, msg)
			}
			i += batchSize
			err := writer.WriteMessages(ctx, batch...)
			if err != nil {
				// кафка упала, надо об этом сказать
				log.Errorf("error occurred in thread %v", threadId)
				log.Errorln(err)
				holder.Threads[threadId].StatusChan <- thread.DEAD
				log.Debugf("goroutine %v write to channel", threadId)
				// do I have a gazing bug here or is it just my fears? I wasn't able to determine
				holder.AppendBuffer(threadId, data...)
			} else {
				holder.Threads[threadId].StatusChan <- thread.OK
				if i%100 == 0 {
					log.Debugf("thread %v had sent %v messages", threadId, i)
				}
			}
		}
		log.Debugf("thread %v send finishing signal", threadId)
		holder.Threads[threadId].StatusChan <- thread.FINISHED
		close(holder.Threads[threadId].StatusChan)
	}
}

func StartWriting(conn *kafka.Conn, conf config.Config) {
	var holder thread.ThreadsHolder
	ctx, StopThreads := context.WithCancel(context.Background())
	for i := 0; i < conf.MaxThreads; i++ {
		s := make(chan thread.Status, 100)
		holder.Mu = append(holder.Mu, &sync.RWMutex{})
		holder.Threads = append(holder.Threads, thread.Thread{IsDone: false, MsgBuffer: []string{}, DumpPath: conf.DumpDir + fmt.Sprintf("thread%v_dump.txt", i), StatusChan: s, MaxBufSize: conf.MaxBufSize})
	}
	writer := &kafka.Writer{Addr: kafka.TCP(conf.Kafka), Topic: conf.KafkaTopic, Balancer: &kafka.Hash{}, WriteTimeout: 1 * time.Second, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true, BatchSize: conf.MsgBatchSize}
	defer func(writer *kafka.Writer) {
		err := writer.Close()
		if err != nil {
			log.Errorln(err)
		}
	}(writer)
	for i := 0; i < conf.MaxThreads; i++ {
		go writeToKafka(ctx, &holder, writer, conf.MaxMessagesPerThread, conf.MsgBatchSize, i)
	}
	c := make(chan int)
	go StartArbitr(conf, &holder, c)
	log.Infoln("all goroutines are running")
	res := <-c
	if res == ALLDEAD {
		// TODO: I have to keep listening to messages and dumping them, while allowing to be restarted and reconnected
		log.Errorln("system dead.")
	} else if res == ALLRESTART {
		// TODO: restart all
	} else {
		log.Infoln("all good, all messages sent!")
	}
	StopThreads()
}

func StartArbitr(conf config.Config, holder *thread.ThreadsHolder, resChan chan int) {
	deadCnt := 0
	for {
		if holder.CountType(thread.FINISHED) == len(holder.Threads) {
			log.Infoln("finish arbitr")
			resChan <- ALLOK
			return
		}
		deadCnt = holder.CountType(thread.DEAD)
		if deadCnt > conf.MaxDeadThreads {
			log.Errorf("there are %v dead threads but maximum of %v is allowed", deadCnt, conf.MaxDeadThreads)
			break
		}
	}
	log.Infoln("starting retry")
	retrySuccessfull := false
	ticker := time.NewTicker(1 * time.Second)
	attempts := 0
	for range ticker.C {
		log.Infof("retry attempt %v", attempts)
		attempts++
		if attempts > conf.MaxDeadTimeOut {
			break
		}
		newConn, err := kafka.DialLeader(context.Background(), "tcp", conf.Kafka, conf.KafkaTopic, conf.KafkaPartition)
		if err != nil {
			log.Infof("fail!")
			retrySuccessfull = false
			continue
		}
		_, err = newConn.ReadPartitions()
		if err != nil {
			log.Infof("fail!")
			retrySuccessfull = false
		} else {
			log.Infof("success!")
			retrySuccessfull = true
			ticker.Stop()
			break
		}
	}
	// these are errors, so they would be visible with higher logging levels
	log.Errorf("retry success is %v", retrySuccessfull)
	if !retrySuccessfull {
		log.Errorln("retry failed, killing all threads")
		resChan <- ALLDEAD
		return
	} else {
		log.Errorln("retry ok, restarting everything")
		resChan <- ALLRESTART
		return
	}
}
