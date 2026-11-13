//go:build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/gasmod/gas"
)

// A FIFO group stays held while any message of it is in flight, also when
// another message of it is made visible again. The SQS Developer Guide,
// FIFO queue delivery logic, Receiving messages: "you can't receive
// additional messages from the same message group ID in subsequent requests
// until: The currently received messages are deleted, or They become
// visible again"; and Handling visibility timeout: "No additional messages
// from the same message group ID are returned until the first message is
// deleted or becomes visible again." The design: "a group is held while any
// of its messages is in flight".

// receivePair sends m1 and m2 to one group and receives both in one call.
func receivePair(t *testing.T, c *sqs.Client, u string) (m1, m2 types.Message) {
	t.Helper()
	sendFIFO(t, c, u, "m1", "g", "1")
	sendFIFO(t, c, u, "m2", "g", "2")
	msgs := receive(t, c, u, 10)
	requireSlice(t, bodies(msgs), []string{"m1", "m2"}, "one receive of both")
	return msgs[0], msgs[1]
}

func TestFIFOGroupHeldWhileTheOtherIsInFlight(t *testing.T) {
	c, _ := newClient(t)

	t.Run("m1 made visible while m2 is in flight", func(t *testing.T) {
		u := createFIFO(t, c, "first-back.fifo", nil)
		m1, m2 := receivePair(t, c, u)

		changeVisibility(t, c, u, m1, 0)
		requireEmpty(t, c, u, "m1 is visible, m2 still in flight")
		requireCounts(t, c, u, counts{visible: 1, notVisible: 1}, "while the group is held")

		deleteMessage(t, c, u, m2)
		again := receive(t, c, u, 10)
		requireSlice(t, bodies(again), []string{"m1"}, "after m2 is deleted")
		requireEqual(t, receiveCount(t, again[0]), 2, "m1's receive count")
	})

	t.Run("m2 made visible while m1 is in flight", func(t *testing.T) {
		u := createFIFO(t, c, "second-back.fifo", nil)
		m1, m2 := receivePair(t, c, u)

		changeVisibility(t, c, u, m2, 0)
		requireEmpty(t, c, u, "m2 is visible, m1 still in flight")

		deleteMessage(t, c, u, m1)
		again := receive(t, c, u, 10)
		requireSlice(t, bodies(again), []string{"m2"}, "after m1 is deleted")
	})

	t.Run("both made visible come back in order", func(t *testing.T) {
		u := createFIFO(t, c, "both-back.fifo", nil)
		m1, m2 := receivePair(t, c, u)
		changeVisibility(t, c, u, m2, 0)
		changeVisibility(t, c, u, m1, 0)
		requireSlice(
			t,
			bodies(receive(t, c, u, 10)),
			[]string{"m1", "m2"},
			"after both are released",
		)
	})

	t.Run("another group is not held", func(t *testing.T) {
		u := createFIFO(t, c, "other-group.fifo", nil)
		m1, _ := receivePair(t, c, u)
		changeVisibility(t, c, u, m1, 0)
		sendFIFO(t, c, u, "x1", "other", "3")
		requireSlice(t, bodies(receive(t, c, u, 10)), []string{"x1"}, "only the other group")
	})
}

func TestFIFOGroupHeldWhenTheOtherTimesOut(t *testing.T) {
	// The same rule when m1 becomes visible by its own timeout: a receive
	// with a short VisibilityTimeout for m1, then m2 kept in flight longer.
	c, _ := newClient(t)
	u := createFIFO(t, c, "timed-out.fifo", nil)
	m1, m2 := receivePair(t, c, u)
	changeVisibility(t, c, u, m1, 1)
	time.Sleep(1500 * time.Millisecond)

	requireEmpty(t, c, u, "m1 timed out, m2 still in flight")
	deleteMessage(t, c, u, m2)
	requireSlice(t, bodies(receive(t, c, u, 10)), []string{"m1"}, "after m2 is deleted")
}

func TestGasQueueFIFONackKeepsTheGroupHeld(t *testing.T) {
	// Through Gas's queue/sqs: Nack is ChangeMessageVisibility 0 (package
	// documentation, Acknowledging).
	c, srv := newClient(t)
	createFIFO(t, c, "gas-held.fifo", nil)
	q := gasQueue(t, srv, 0)
	ctx := t.Context()
	for _, b := range []string{"m1", "m2"} {
		requireNoErr(t, q.Enqueue(ctx, "gas-held.fifo", []byte(b),
			gas.WithGroupID("g"), gas.WithDedupeID(b)), "enqueue "+b)
	}

	jobs, err := q.Dequeue(ctx, "gas-held.fifo", 10, time.Second)
	requireNoErr(t, err, "dequeue")
	requireSlice(t, jobBodies(jobs), []string{"m1", "m2"}, "one dequeue of both")

	requireNoErr(t, q.Nack(ctx, "gas-held.fifo", jobs[0]), "nack m1")
	held, err := q.Dequeue(ctx, "gas-held.fifo", 10, 0)
	requireNoErr(t, err, "dequeue while m2 is leased")
	requireEqual(t, len(held), 0, "jobs while m2 is leased")

	requireNoErr(t, q.Ack(ctx, "gas-held.fifo", jobs[1]), "ack m2")
	again, err := q.Dequeue(ctx, "gas-held.fifo", 10, 0)
	requireNoErr(t, err, "dequeue after the ack")
	requireSlice(t, jobBodies(again), []string{"m1"}, "after m2 is acked")
}

func jobBodies(jobs []gas.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, string(j.Body))
	}
	return out
}
