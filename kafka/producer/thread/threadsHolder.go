package thread

import (
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"sync"
)

type ThreadsHolder struct {
	Mu      []*sync.RWMutex
	Threads []Thread
}

func (t *ThreadsHolder) CountType(sig Status) int {
	cnt := 0
	for i := range t.Threads {
		t.Mu[i].RLock()
		log.Debugf("mutex %v locked for reading by arbitr\n", i)
		s := <-t.Threads[i].StatusChan
		if s == sig {
			cnt++
		}
		t.Mu[i].RUnlock()
		log.Debugf("mutex %v unlocked by arbitr\n", i)
	}
	return cnt
}

func (t *ThreadsHolder) FinishThread(id int) {
	log.Infof("finishing thread %v", id)
	t.Mu[id].Lock()
	defer t.Mu[id].Unlock()
	t.Threads[id].FinishThread()
}

func (t *ThreadsHolder) AppendBuffer(id int, msg ...string) {
	t.Mu[id].Lock()
	log.Debugf("mutex %v locked for writing by goroutine %v\n", id, id)
	defer func() {
		t.Mu[id].Unlock()
		log.Debugf("mutex %v unlocked by goroutine %v\n", id, id)
	}()
	t.Threads[id].AppendBuffer(msg...)
}

func (t *ThreadsHolder) ReadBatchFromBuffer(id int, batchSz int) []kafka.Message {
	t.Mu[id].RLock()
	log.Debugf("mutex %v locked for reading by goroutine %v", id, id)
	msgs := t.Threads[id].GetBatchFromBuffer(batchSz)
	t.Mu[id].RUnlock()
	log.Debugf("mutex %v unlocked for reading by goroutine %v", id, id)
	var res []kafka.Message
	for _, msg := range msgs {
		res = append(res, kafka.Message{Value: []byte(msg)})
	}
	return res
}
