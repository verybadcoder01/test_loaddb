package internal

import (
	"bufio"
	"context"
	"dbload/kafka/config"
	"dbload/kafka/producer/thread"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
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
	for i := 0; i < maxMsg; {
		select {
		case <-ctx.Done():
			return
		default:
			var batch []kafka.Message
			var data []string // in case we need dumping
			// let's check the buffer
			prevBatch := holder.ReadBatchFromBuffer(threadId, batchSize)
			for j := 0; j < batchSize; j++ {
				// but we still have to fetch new messages
				msg := GetMessage()
				batch = append(batch, kafka.Message{Value: []byte(msg)})
				data = append(data, msg)
			}
			i += batchSize
			var err error
			// if there are enough messages in the buffer we should try to write them
			if len(prevBatch) == batchSize {
				err = writer.WriteMessages(ctx, prevBatch...)
			} else {
				// buffer is empty, writing new ones
				err = writer.WriteMessages(ctx, batch...)
			}
			if err != nil {
				// kafka is down
				log.Errorf("error occurred in thread %v", threadId)
				log.Errorln(err)
				holder.Threads[threadId].StatusChan <- thread.DEAD
				log.Debugf("goroutine %v write to channel", threadId)
				// it doesn't matter if we tried writing messages from the buffer or the new ones, we should always save the new ones
				holder.AppendBuffer(threadId, data...)
			} else {
				holder.Threads[threadId].StatusChan <- thread.OK
				if i%100 == 0 {
					log.Debugf("thread %v had sent %v messages", threadId, i)
				}
			}
		}
	}
	log.Debugf("thread %v send finishing signal", threadId)
	holder.Threads[threadId].StatusChan <- thread.FINISHED
	close(holder.Threads[threadId].StatusChan)
}

func keepListening(ctx context.Context, holder *thread.ThreadsHolder, batchSz int, threadId int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var data []string
			for i := 0; i < batchSz; i++ {
				msg := GetMessage()
				data = append(data, msg)
			}
			holder.AppendBuffer(threadId, data...)
		}
	}
}

func DoRestart() {
	// TODO
}

func StartWriting(conn *kafka.Conn, conf config.Config) {
	var holder thread.ThreadsHolder
	ctx, StopThreads := context.WithCancel(context.Background())
	for i := 0; i < conf.MaxThreads; i++ {
		s := make(chan thread.Status, 100)
		holder.Mu = append(holder.Mu, &sync.RWMutex{})
		holder.Threads = append(holder.Threads, thread.Thread{IsDone: false, MsgBuffer: []string{}, DumpPath: conf.DumpDir + fmt.Sprintf("thread%v_dump.txt", i), StatusChan: s, MaxBufSize: conf.MaxBufSize, MaxDumpSize: conf.MaxDumpSize})
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
	arbitrResChan := make(chan int)
	go StartArbitr(conf, &holder, arbitrResChan)
	log.Infoln("all goroutines are running")
	// blocks here until arbitr is done
	res := <-arbitrResChan
	StopThreads()
	if res == ALLDEAD {
		ctxL, stopListening := context.WithCancel(context.Background())
		for i := 0; i < conf.MaxThreads; i++ {
			go keepListening(ctxL, &holder, conf.MsgBatchSize, i)
		}
		log.Errorln("Messages are no more going through, only listening and dumping.")
		reader := bufio.NewReader(os.Stdin)
		// until inputted will block and wait forever
		command, _ := reader.ReadString('\n')
		stopListening()
		if command == "restart\n" {
			DoRestart()
		} else {
			log.Errorln("all dead.")
		}
	} else if res == ALLRESTART {
		DoRestart()
	} else {
		log.Infoln("all good, all messages sent!")
	}
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
