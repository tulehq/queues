package sqlimpl

import (
	"database/sql"
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

// fillGroups sends per messages to each of groups FIFO groups of queue,
// g0, g1 and so on, a message of each group in turn.
func fillGroups(b *testing.B, s *Service, queue string, groups, per int) {
	b.Helper()
	batch := make([]contracts.SendInput, 0, 100)
	for j := range per {
		for g := range groups {
			id := strconv.Itoa(g) + "-" + strconv.Itoa(j)
			batch = append(batch, contracts.SendInput{
				Body: "m", GroupID: "g" + strconv.Itoa(g), DeduplicationID: id,
			})
			if len(batch) == cap(batch) || (j == per-1 && g == groups-1) {
				if _, err := s.SendMessages(b.Context(), queue, batch); err != nil {
					b.Fatalf("fill: %v", err)
				}
				batch = batch[:0]
			}
		}
	}
}

// holdHeads puts in flight, until after the benchmark, the first message
// of each of the queue q's groups, which holds the group.
func holdHeads(b *testing.B, s *Service, c *clock, q int64, groups ...string) {
	b.Helper()
	if err := s.inTx(b.Context(), func(tx *sql.Tx) error {
		for _, g := range groups {
			if err := exec(b.Context(), tx,
				`UPDATE messages SET receive_count = 1, visible_at = ?
				 WHERE id = (SELECT MIN(id) FROM messages WHERE queue_id = ? AND group_id = ?)`,
				stamp(c.Now().Add(contracts.MaxVisibility)), q, g); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("hold: %v", err)
	}
}

// BenchmarkMoveFIFOStep selects the messages a move task's step would move
// from a FIFO queue: one group of 20,000 held by its head in flight, with
// one other group ready, which a task waiting for the held group reads
// again every heldPause; 20,000 groups of one message; 1,000 groups of 20,
// every tenth held; and a group held by its head with 300, or 2,000,
// messages behind it, ahead of 20,000 groups of one message.
func BenchmarkMoveFIFOStep(b *testing.B) {
	for _, tt := range fifoMoveShapes() {
		b.Run(tt.name, func(b *testing.B) {
			s, c := newStore(b)
			createQueue(b, s, "q.fifo", true, nil)
			var t moveTaskRow
			if err := s.inTx(b.Context(), func(tx *sql.Tx) error {
				q, err := loadQueue(b.Context(), tx, "q.fifo")
				if err != nil {
					return err
				}
				t = moveTaskRow{queueID: q.id, upTo: 1 << 40, scan: new(int)}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			tt.fill(b, s, c, t.queueID)
			step := func() {
				if err := s.inTx(b.Context(), func(tx *sql.Tx) error {
					msgs, _, err := toMove(b.Context(), tx, &t, moveBatch+1, c.Now())
					if err == nil && len(msgs) != tt.want {
						b.Fatalf("to move = %d messages, want %d", len(msgs), tt.want)
					}
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
			// The task's first step counts the groups, once.
			step()
			b.ResetTimer()
			for range b.N {
				step()
			}
		})
	}
}

// fifoMoveShape is a FIFO queue BenchmarkMoveFIFOStep steps on: fill fills
// the queue q, and a step finds want messages to move.
type fifoMoveShape struct {
	fill func(b *testing.B, s *Service, c *clock, q int64)
	name string
	want int
}

// fifoMoveShapes are the queues BenchmarkMoveFIFOStep steps on.
func fifoMoveShapes() []fifoMoveShape {
	return []fifoMoveShape{
		{name: "held-group", want: 1, fill: func(b *testing.B, s *Service, c *clock, q int64) {
			b.Helper()
			fill(b, s, "q.fifo", "big", 20000)
			fill(b, s, "q.fifo", "free", 1)
			holdHeads(b, s, c, q, "big")
		}},
		{name: "groups", want: moveBatch + 1, fill: func(b *testing.B, s *Service, _ *clock,
			_ int64,
		) {
			b.Helper()
			fillGroups(b, s, "q.fifo", 20000, 1)
		}},
		{name: "mixed", want: moveBatch + 1, fill: func(b *testing.B, s *Service, c *clock,
			q int64,
		) {
			b.Helper()
			fillGroups(b, s, "q.fifo", 1000, 20)
			held := make([]string, 0, 100)
			for g := 0; g < 1000; g += 10 {
				held = append(held, "g"+strconv.Itoa(g))
			}
			holdHeads(b, s, c, q, held...)
		}},
		{name: "backlog-300", want: moveBatch + 1, fill: backlogThenGroups(300)},
		{name: "backlog-2000", want: moveBatch + 1, fill: backlogThenGroups(2000)},
	}
}

// backlogThenGroups fills a FIFO queue with a group held by its head in
// flight with n messages behind it, and then 20,000 groups of one message.
func backlogThenGroups(n int) func(b *testing.B, s *Service, c *clock, q int64) {
	return func(b *testing.B, s *Service, c *clock, q int64) {
		b.Helper()
		fill(b, s, "q.fifo", "held", n+1)
		holdHeads(b, s, c, q, "held")
		fillGroups(b, s, "q.fifo", 20000, 1)
	}
}
