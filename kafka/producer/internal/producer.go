package internal

import (
	"bufio"
	"context"
	"os"
	"sort"
	"time"

	"dbload/config"
	"dbload/kafka/message"
	"dbload/kafka/producer/thread"
	"dbload/postgres/database"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

// arbitr statuses
const (
	ALLDEAD = iota
	ALLRESTART
	ALLOK
)

func writeToKafka(ctx context.Context, db database.Database, logger *log.Logger, holder *thread.ThreadsHolder, writer *kafka.Writer, maxMsg int, batchSize int, threadID int, requireSort bool) {
	defer func(holder *thread.ThreadsHolder) {
		logger.Debugf("thread %v send finishing signal", threadID)
		holder.FinishThread(threadID)
	}(holder)
	for i := 0; i < maxMsg; {
		select {
		case <-ctx.Done():
			return
		default:
			var batch []kafka.Message
			var data []message.Message // in case we need dumping
			// let's check the buffer
			prevBatch := holder.ReadBatchFromBuffer(threadID, batchSize)
			// but we still have to fetch new messages
			msg := db.GetMessages(threadID, batchSize)
			for j := 0; j < batchSize; j++ {
				batch = append(batch, msg[j].ToKafkaMessage())
				data = append(data, msg[j])
			}
			i += batchSize
			// if there are enough messages in the buffer we should try to write them, then write the new ones
			if len(prevBatch) == batchSize {
				if !requireSort {
					err := writer.WriteMessages(ctx, prevBatch...)
					if err != nil {
						logger.Errorf("error occurred in thread %v", threadID)
						logger.Errorln(err)
					}
				} else {
					holder.Threads[threadID].SorterChan <- data
				}
			}
			// check if we need any timestamp sorting
			if !requireSort {
				err := writer.WriteMessages(ctx, batch...)
				if err != nil {
					// kafka is down
					logger.Errorf("error occurred in thread %v", threadID)
					logger.Errorln(err)
					holder.Threads[threadID].StatusChan <- thread.DEAD
					logger.Debugf("goroutine %v write to channel", threadID)
					// it doesn't matter if we tried writing messages from the buffer or the new ones, we should always save the new ones
					holder.AppendBuffer(threadID, data...)
				} else {
					holder.Threads[threadID].StatusChan <- thread.OK
					if i%100 == 0 {
						logger.Debugf("thread %v had sent %v messages", threadID, i)
					}
				}
			} else {
				// pass the values to sorter thread
				holder.Threads[threadID].SorterChan <- data
			}
		}
	}
}

func timeStampSorter(ctx context.Context, logger *log.Logger, writer *kafka.Writer, holder *thread.ThreadsHolder, sendBatchSize int) {
	var cur []message.Message
	sendCurrent := func() error {
		km := make([]kafka.Message, len(cur))
		for i, msg := range cur {
			km[i] = msg.ToKafkaMessage()
			km[i].Key = []byte(msg.GetTimestamp().String())
		}
		logger.Debugln("sending batch from timestamp sorter")
		err := writer.WriteMessages(ctx, km...)
		return err
	}
	for {
		select {
		case <-ctx.Done():
			// finishing here, let's send what we have
			_ = sendCurrent()
			return
		default:
			for _, th := range holder.Threads {
				cur = append(cur, <-th.SorterChan...)
			}
			if len(cur) >= sendBatchSize {
				sort.Slice(cur, func(i, j int) bool { return cur[i].GetTimestamp().Before(cur[j].GetTimestamp()) })
				err := sendCurrent()
				cur = []message.Message{}
				if err != nil {
					logger.Errorln("error occurred in timestamp sorted while sending: " + err.Error())
					// I guess that'll do
					for _, t := range holder.Threads {
						t.StatusChan <- thread.DEAD
					}
				}
			}
		}
	}
}

func keepListening(ctx context.Context, db database.Database, holder *thread.ThreadsHolder, batchSz int, threadID int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg := db.GetMessages(threadID, batchSz)
			holder.AppendBuffer(threadID, msg...)
		}
	}
}

func startWork(ctx context.Context, logger *log.Logger, db database.Database, conf config.Config, holder *thread.ThreadsHolder) int {
	writer := &kafka.Writer{
		Addr: kafka.TCP(conf.Kafka.Brokers[0]), Topic: conf.Kafka.Topic, Balancer: &kafka.Hash{},
		WriteTimeout: time.Duration(conf.Producer.WriteTimeOutSec) * time.Second,
		RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true, BatchSize: conf.Producer.MsgBatchSize,
	}
	defer func(writer *kafka.Writer) {
		err := writer.Close()
		if err != nil {
			logger.Errorln(err)
		}
	}(writer)
	if conf.Producer.RequireTimestampSort {
		go timeStampSorter(ctx, logger, writer, holder, conf.Producer.SortedMsgBatchSize)
	}
	for i := 0; i < conf.Performance.MaxThreads; i++ {
		go writeToKafka(ctx, db, logger, holder, writer, conf.Performance.MaxMessagesPerThread, conf.Producer.MsgBatchSize, i, conf.Producer.RequireTimestampSort)
	}
	arbitrResChan := make(chan int)
	go StartArbitr(logger, conf, holder, arbitrResChan)
	logger.Infoln("all goroutines are running")
	// blocks here until arbitr is done
	res := <-arbitrResChan
	return res
}

func StartWriting(ctx context.Context, db database.Database, logger *log.Logger, conf config.Config, holder *thread.ThreadsHolder) {
	for {
		ctx, StopThreads := context.WithCancel(ctx)
		res := startWork(ctx, logger, db, conf, holder)
		StopThreads()
		if res == ALLDEAD { //nolint // would require a lot of messing with break labels, not worth it
			ctxL, stopListening := context.WithCancel(context.Background())
			for i := 0; i < conf.Performance.MaxThreads; i++ {
				go keepListening(ctxL, db, holder, conf.Producer.MsgBatchSize, i)
			}
			logger.Errorln("Messages are no more going through, only listening and dumping.")
			cmdReader := bufio.NewReader(os.Stdin)
			// until inputted will block and wait forever
			command, _ := cmdReader.ReadString('\n')
			stopListening()
			if command == "restart\n" {
				logger.Infof("restart command read, starting now")
			} else {
				logger.Errorln("all dead.")
				break
			}
		} else if res == ALLRESTART {
			logger.Infoln("going to restart now")
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
		finished = maxFunc(holder.CountType(thread.FINISHED), finished)
		if finished == holder.Len() {
			logger.Infoln("finish arbitr")
			resChan <- ALLOK
			return
		}
		deadCnt = holder.CountType(thread.DEAD)
		if deadCnt > conf.Performance.MaxDeadThreads {
			logger.Errorf("there are %v dead threads but maximum of %v is allowed", deadCnt, conf.Performance.MaxDeadThreads)
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
		if attempts > conf.Performance.MaxDeadTimeOut {
			break
		}
		retrySuccessfull = attemptConnect(conf.Kafka.Brokers[0], conf.Kafka.Topic, 0)
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
