package sqlimpl

import "sync"

// signals wake the receives waiting on a queue. A waiter takes the queue's
// channel before it looks for messages, so a message sent after it looked
// closes a channel it holds: no send between the look and the wait is
// missed. Queues are keyed by name, as receives name them.
//
// Once stopped, for the server's shutdown, every channel is closed and
// stays so, and waiting receives return what they have, nothing, rather
// than hold the shutdown up for the rest of their wait.
type signals struct {
	chans   map[string]chan struct{}
	stopped chan struct{}
	mu      sync.Mutex
}

func newSignals() *signals {
	return &signals{chans: map[string]chan struct{}{}, stopped: make(chan struct{})}
}

// wait returns the channel the next signal of queue name closes.
func (s *signals) wait(name string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isStopped() {
		return s.stopped
	}
	ch, ok := s.chans[name]
	if !ok {
		ch = make(chan struct{})
		s.chans[name] = ch
	}
	return ch
}

// signal wakes every receive waiting on the named queues: messages may have
// become available, or the queue is gone.
func (s *signals) signal(names ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range names {
		if ch, ok := s.chans[name]; ok {
			close(ch)
			delete(s.chans, name)
		}
	}
}

// stop wakes every waiting receive for good.
func (s *signals) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isStopped() {
		return
	}
	close(s.stopped)
	for name, ch := range s.chans {
		close(ch)
		delete(s.chans, name)
	}
}

func (s *signals) isStopped() bool {
	select {
	case <-s.stopped:
		return true
	default:
		return false
	}
}
