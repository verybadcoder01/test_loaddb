package thread

import (
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
	t.Mu[id].Lock()
	log.Infof("finishing thread %v", id)
	defer t.Mu[id].Unlock()
	t.Threads[id].FinishThread()
}

func (t *ThreadsHolder) AppendBuffer(id int, msg string) {
	t.Mu[id].Lock()
	log.Debugf("mutex %v locked for writing by goroutine %v\n", id, id)
	defer func() {
		t.Mu[id].Unlock()
		log.Debugf("mutex %v unlocked by goroutine %v\n", id, id)
	}()
	t.Threads[id].AppendBuffer(msg)
}
