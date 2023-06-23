package thread

import (
	"log"
	"sync"
)

type ThreadsHolder struct {
	Mu      sync.Mutex
	Threads []Thread
}

func (t *ThreadsHolder) CountType(sig Status) int {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	cnt := 0
	for _, th := range t.Threads {
		s := <-th.StatusChan
		if s == sig {
			cnt++
		}
	}
	return cnt
}

func (t *ThreadsHolder) FinishThread(id int) {
	log.Printf("finishing thread %v\n", id)
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Threads[id].FinishThread()
}
