package sqlimpl

import (
	"strconv"
	"testing"

	"tule.dev/queues/internal/contracts"
)

// The receive and send benchmarks run on queues of 100 and of 20,000
// messages: their cost should not grow with what else the queue holds.
var benchSizes = []int{100, 20000}

// fill sends n messages to queue, in batches, in FIFO group g when g is set.
func fill(b *testing.B, s *Service, queue, g string, n int) {
	b.Helper()
	for i := 0; i < n; i += 100 {
		batch := make([]contracts.SendInput, 0, 100)
		for j := i; j < min(i+100, n); j++ {
			in := contracts.SendInput{Body: "m"}
			if g != "" {
				in.GroupID, in.DeduplicationID = g, g+strconv.Itoa(j)
			}
			batch = append(batch, in)
		}
		if _, err := s.SendMessages(b.Context(), queue, batch); err != nil {
			b.Fatalf("fill: %v", err)
		}
	}
}

// BenchmarkReceiveFIFOBehindAHeldGroup receives from a FIFO queue whose one
// big group is held by its head in flight, with one other group ready.
func BenchmarkReceiveFIFOBehindAHeldGroup(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, _ := newStore(b)
			createQueue(b, s, "q.fifo", true, map[string]string{"VisibilityTimeout": "43200"})
			fill(b, s, "q.fifo", "big", n)
			if _, err := s.ReceiveMessages(b.Context(), "q.fifo",
				contracts.ReceiveInput{Max: 1}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := range b.N {
				b.StopTimer()
				fill(b, s, "q.fifo", "small"+strconv.Itoa(i), 1)
				b.StartTimer()
				got, err := s.ReceiveMessages(b.Context(), "q.fifo", contracts.ReceiveInput{Max: 1})
				if err != nil || len(got) != 1 || got[0].GroupID == "big" {
					b.Fatalf("receive = %v, %v", got, err)
				}
			}
		})
	}
}

// BenchmarkReceiveStandardBehindInFlight receives from a standard queue
// whose messages are all in flight but the one just sent.
func BenchmarkReceiveStandardBehindInFlight(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, _ := newStore(b)
			createQueue(b, s, "q", false, map[string]string{"VisibilityTimeout": "43200"})
			fill(b, s, "q", "", n)
			for range n / 10 {
				if _, err := s.ReceiveMessages(b.Context(), "q",
					contracts.ReceiveInput{Max: 10}); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				fill(b, s, "q", "", 1)
				b.StartTimer()
				got, err := s.ReceiveMessages(b.Context(), "q", contracts.ReceiveInput{Max: 1})
				if err != nil || len(got) != 1 {
					b.Fatalf("receive = %v, %v", got, err)
				}
			}
		})
	}
}

// BenchmarkSendToALargeQueue sends one message to a queue that holds n,
// through the path SendMessage takes: the queue's attributes, then the send.
func BenchmarkSendToALargeQueue(b *testing.B) {
	for _, n := range benchSizes {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, _ := newStore(b)
			createQueue(b, s, "q.fifo", true, nil)
			fill(b, s, "q.fifo", "g", n)
			b.ResetTimer()
			for i := range b.N {
				if _, err := s.QueueInfo(b.Context(), "q.fifo"); err != nil {
					b.Fatal(err)
				}
				if _, err := s.SendMessages(b.Context(), "q.fifo", []contracts.SendInput{{
					Body: "m", GroupID: "g", DeduplicationID: "x" + strconv.Itoa(i),
				}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
