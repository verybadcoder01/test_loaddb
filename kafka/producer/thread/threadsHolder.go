package thread

import (
	"sync"

	"dbload/kafka/message"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type ThreadsHolder struct {
	mutexes []*sync.Mutex
	Threads []Thread
}

func NewThreadsHolder(mu []*sync.Mutex, threads []Thread) ThreadsHolder {
	return ThreadsHolder{mutexes: mu, Threads: threads}
}

func (t *ThreadsHolder) Len() int {
	return len(t.Threads)
}

func (t *ThreadsHolder) CountType(logger *log.Logger, sig Status) int {
	cnt := 0
	for i := range t.Threads {
		logger.Tracef("mutex %v locked for reading by arbitr\n", i)
		s := <-t.Threads[i].StatusChan
		if s == sig {
			cnt++
		}
		logger.Tracef("mutex %v unlocked by arbitr\n", i)
	}
	return cnt
}

func (t *ThreadsHolder) FinishThread(logger *log.Logger, id int) {
	logger.Infof("finishing thread %v", id)
	t.mutexes[id].Lock()
	defer t.mutexes[id].Unlock()
	t.Threads[id].FinishThread(logger)
}

func (t *ThreadsHolder) AppendBuffer(logger *log.Logger, id int, msg ...message.Message) {
	t.mutexes[id].Lock()
	logger.Tracef("mutex %v locked for writing by goroutine %v\n", id, id)
	defer func() {
		t.mutexes[id].Unlock()
		logger.Tracef("mutex %v unlocked by goroutine %v\n", id, id)
	}()
	t.Threads[id].AppendBuffer(logger, msg...)
}

func (t *ThreadsHolder) ReadBatchFromBuffer(logger *log.Logger, id int, batchSz int) []kafka.Message {
	t.mutexes[id].Lock()
	logger.Tracef("mutex %v locked for writing by goroutine %v", id, id)
	msgs := t.Threads[id].ExtractBatchFromBuffer(batchSz)
	t.mutexes[id].Unlock()
	logger.Tracef("mutex %v unlocked by goroutine %v", id, id)
	var res []kafka.Message
	for _, msg := range msgs {
		res = append(res, msg.ToKafkaMessage())
	}
	return res
}
