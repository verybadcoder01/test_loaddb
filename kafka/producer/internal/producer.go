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

func writeToKafka(ctx context.Context, holder *thread.ThreadsHolder, conn *kafka.Conn, maxMsg int, threadId int) {
	defer holder.FinishThread(threadId)
	select {
	case <-ctx.Done():
		return
	default:
		// will never time out
		err := conn.SetWriteDeadline(time.Time{})
		if err != nil {
			log.Errorln(err)
		}
		for i := 0; i < maxMsg; i++ {
			msg := GetMessage()
			_, err = conn.WriteMessages(kafka.Message{Value: []byte(msg)})
			if err != nil {
				// кафка упала, надо об этом сказать
				log.Errorf("error occurred in thread %v\n", threadId)
				log.Errorln(err)
				// TODO: crete listener for this channel
				//holder[threadId].CriticalChan <- customErrors.NewCriticalError(err, threadId)
				holder.Threads[threadId].StatusChan <- thread.DEAD
				log.Debugf("goroutine %v write to channel\n", threadId)
				// do I have a gazing bug here or is it just my fears? I wasn't able to determine
				holder.AppendBuffer(threadId, msg)
			} else {
				holder.Threads[threadId].StatusChan <- thread.OK
			}
		}
		holder.Threads[threadId].StatusChan <- thread.FINISHED
		close(holder.Threads[threadId].StatusChan)
	}
}

func StartWriting(conn *kafka.Conn, conf config.Config) {
	var holder thread.ThreadsHolder
	ctx, StopThreads := context.WithCancel(context.Background())
	for i := 0; i < conf.MaxThreads; i++ {
		c := make(chan error)
		s := make(chan thread.Status, 100)
		holder.Mu = append(holder.Mu, &sync.RWMutex{})
		holder.Threads = append(holder.Threads, thread.Thread{IsDone: false, CriticalChan: c, MsgBuffer: []string{}, DumpPath: conf.DumpFile, StatusChan: s, MaxBufSize: conf.MaxBufSize})
	}
	for i := 0; i < conf.MaxThreads; i++ {
		go writeToKafka(ctx, &holder, conn, conf.MaxMessagesPerThread, i)
	}
	c := make(chan int)
	go StartArbitr(conf, &holder, c)
	log.Infoln("all goroutines are running")
	res := <-c
	if res == ALLDEAD {
		// TODO: I have to keep listening to messages and dumping them, while allowing to be restarted and reconnected
		log.Errorln("system dead.")
	} else if res == ALLRESTART {
		//TODO: restart all
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
		log.Infof("retry attempt %v\n", attempts)
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
	log.Errorf("retry success is %v\n", retrySuccessfull)
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
