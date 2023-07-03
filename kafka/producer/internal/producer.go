package internal

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"dbload/kafka/config"
	"dbload/kafka/message"
	"dbload/kafka/producer/buffer"
	"dbload/kafka/producer/thread"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

func GetMessage() message.Message {
	return &message.SimpleMessage{Value: fmt.Sprintf("message %v\n", rand.Int())} //nolint //It just doesn't matter
}

// arbitr statuses
const (
	ALLDEAD = iota
	ALLRESTART
	ALLOK
)

func writeToKafka(ctx context.Context, logger *log.Logger, holder *thread.ThreadsHolder, writer *kafka.Writer, maxMsg int, batchSize int, threadID int) {
	defer holder.FinishThread(logger, threadID)
	for i := 0; i < maxMsg; {
		select {
		case <-ctx.Done():
			return
		default:
			var batch []kafka.Message
			var data []message.Message // in case we need dumping
			// let's check the buffer
			prevBatch := holder.ReadBatchFromBuffer(logger, threadID, batchSize)
			for j := 0; j < batchSize; j++ {
				// but we still have to fetch new messages
				msg := GetMessage()
				batch = append(batch, msg.ToKafkaMessage())
				data = append(data, msg)
			}
			i += batchSize
			// if there are enough messages in the buffer we should try to write them, then write the new ones
			if len(prevBatch) == batchSize {
				err := writer.WriteMessages(ctx, prevBatch...)
				if err != nil {
					logger.Errorf("error occurred in thread %v", threadID)
					logger.Errorln(err)
				}
			}
			err := writer.WriteMessages(ctx, batch...)
			if err != nil {
				// kafka is down
				logger.Errorf("error occurred in thread %v", threadID)
				logger.Errorln(err)
				holder.Threads[threadID].StatusChan <- thread.DEAD
				logger.Debugf("goroutine %v write to channel", threadID)
				// it doesn't matter if we tried writing messages from the buffer or the new ones, we should always save the new ones
				holder.AppendBuffer(logger, threadID, data...)
			} else {
				holder.Threads[threadID].StatusChan <- thread.OK
				if i%100 == 0 {
					logger.Debugf("thread %v had sent %v messages", threadID, i)
				}
			}
		}
	}
	logger.Debugf("thread %v send finishing signal", threadID)
	holder.Threads[threadID].StatusChan <- thread.FINISHED
	close(holder.Threads[threadID].StatusChan)
}

func keepListening(ctx context.Context, logger *log.Logger, holder *thread.ThreadsHolder, batchSz int, threadID int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var data []message.Message
			for i := 0; i < batchSz; i++ {
				msg := GetMessage()
				data = append(data, msg)
			}
			holder.AppendBuffer(logger, threadID, data...)
		}
	}
}

func startWork(ctx context.Context, logger *log.Logger, conf config.Config, holder *thread.ThreadsHolder) int {
	writer := &kafka.Writer{
		Addr: kafka.TCP(conf.Kafka), Topic: conf.KafkaTopic, Balancer: &kafka.Hash{}, WriteTimeout: 1 * time.Second,
		RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true, BatchSize: conf.MsgBatchSize,
	}
	defer func(writer *kafka.Writer) {
		err := writer.Close()
		if err != nil {
			logger.Errorln(err)
		}
	}(writer)
	for i := 0; i < conf.MaxThreads; i++ {
		go writeToKafka(ctx, logger, holder, writer, conf.MaxMessagesPerThread, conf.MsgBatchSize, i)
	}
	arbitrResChan := make(chan int)
	go StartArbitr(logger, conf, holder, arbitrResChan)
	logger.Infoln("all goroutines are running")
	// blocks here until arbitr is done
	res := <-arbitrResChan
	return res
}

func StartWriting(logger *log.Logger, conf config.Config) {
	var tmpMuList []*sync.Mutex
	var tmpThreadsList []thread.Thread
	for i := 0; i < conf.MaxThreads; i++ {
		s := make(chan thread.Status, 100)
		tmpMuList = append(tmpMuList, &sync.Mutex{})
		tmpThreadsList = append(tmpThreadsList,
			thread.NewThread(s,
				buffer.NewDequeBuffer(buffer.NewSimpleDumper(conf.DumpDir+fmt.Sprintf("thread%v_dump.txt", i), int64(conf.MaxDumpSize)),
					conf.MaxBufSize,
				)))
	}
	holder := thread.NewThreadsHolder(tmpMuList, tmpThreadsList)
	for {
		ctx, StopThreads := context.WithCancel(context.Background())
		res := startWork(ctx, logger, conf, &holder)
		StopThreads()
		if res == ALLDEAD { //nolint // would require a lot of messing with break labels, not worth it
			ctxL, stopListening := context.WithCancel(context.Background())
			for i := 0; i < conf.MaxThreads; i++ {
				go keepListening(ctxL, logger, &holder, conf.MsgBatchSize, i)
			}
			logger.Errorln("Messages are no more going through, only listening and dumping.")
			reader := bufio.NewReader(os.Stdin)
			// until inputted will block and wait forever
			command, _ := reader.ReadString('\n')
			stopListening()
			if command == "restart\n" {
				logger.Infof("restart command read, starting now")
			} else {
				logger.Errorln("all dead.")
				break
			}
		} else if res == ALLRESTART {
			logger.Infof("going to restart now")
		} else {
			logger.Infoln("all good, all messages sent!")
			break
		}
	}
}

func attemptConnect(addr string, topic string, partition int) bool {
	newConn, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, partition)
	if err != nil {
		return false
	}
	_, err = newConn.ReadPartitions()
	return err == nil
}

func StartArbitr(logger *log.Logger, conf config.Config, holder *thread.ThreadsHolder, resChan chan<- int) {
	deadCnt := 0
	finished := 0
	maxFunc := func(a int, b int) int {
		if a > b {
			return a
		}
		return b
	}
	for {
		// since threads might not finish at the same time, we need to count total amount
		finished = maxFunc(holder.CountType(logger, thread.FINISHED), finished)
		if finished == holder.Len() {
			logger.Infoln("finish arbitr")
			resChan <- ALLOK
			return
		}
		deadCnt = holder.CountType(logger, thread.DEAD)
		if deadCnt > conf.MaxDeadThreads {
			logger.Errorf("there are %v dead threads but maximum of %v is allowed", deadCnt, conf.MaxDeadThreads)
			break
		}
	}
	logger.Infoln("starting retry")
	retrySuccessfull := false
	ticker := time.NewTicker(1 * time.Second)
	attempts := 0
	for range ticker.C {
		logger.Infof("retry attempt %v", attempts)
		attempts++
		if attempts > conf.MaxDeadTimeOut {
			break
		}
		retrySuccessfull = attemptConnect(conf.Kafka, conf.KafkaTopic, conf.KafkaPartition)
		if retrySuccessfull {
			logger.Infof("success!")
			ticker.Stop()
			break
		} else {
			logger.Infof("fail!")
		}
	}
	// these are errors, so they would be visible with higher logging levels
	logger.Errorf("retry success is %v", retrySuccessfull)
	if !retrySuccessfull {
		logger.Errorln("retry failed, killing all threads")
		resChan <- ALLDEAD
	} else {
		logger.Errorln("retry ok, restarting everything")
		resChan <- ALLRESTART
	}
}
