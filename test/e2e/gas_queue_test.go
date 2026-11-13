//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gasmod/gas"
	"github.com/gasmod/gas/queue"
	gassqs "github.com/gasmod/gas/queue/sqs"
)

// Gas's queue/sqs store, unchanged, against queues. Its package
// documentation: Enqueue is SendMessage, Dequeue is ReceiveMessage with an
// explicit WaitTimeSeconds of 0 for a short poll, Ack is DeleteMessage, and
// Nack and Extend are ChangeMessageVisibility; the store resolves a queue
// name with GetQueueUrl and creates none.

// gasQueue returns an initialized Gas SQS store pointed at srv, with
// visibility the Dequeue visibility timeout (0 for the queue's own).
func gasQueue(t *testing.T, srv *server, visibility time.Duration) *gassqs.Service {
	t.Helper()

	cfg := gassqs.DefaultConfig()
	cfg.Queue.Region = region
	cfg.Queue.Endpoint = srv.URL
	cfg.Queue.AccessKeyID = "test"
	cfg.Queue.SecretAccessKey = gassqs.Secret("test")
	cfg.Queue.MaxRetries = 0
	cfg.Queue.VisibilityTimeout = visibility

	q, err := gassqs.New(gassqs.WithConfig(cfg))(nil, &gas.NopLogger{})
	requireNoErr(t, err, "new gas queue")
	requireNoErr(t, q.Init(t.Context()), "init gas queue")
	t.Cleanup(func() { requireNoErr(t, q.Close(context.Background()), "close gas queue") })
	return q
}

func TestGasQueueSendReceiveAck(t *testing.T) {
	c, srv := newClient(t)
	u := createQueue(t, c, "orders", nil)
	q := gasQueue(t, srv, 0)

	requireNoErr(t, q.Enqueue(t.Context(), "orders", []byte(`{"id":42}`),
		gas.WithJobAttributes(map[string]string{"kind": "order"})), "enqueue")

	jobs, err := q.Dequeue(t.Context(), "orders", 10, time.Second)
	requireNoErr(t, err, "dequeue")
	requireEqual(t, len(jobs), 1, "jobs")
	j := jobs[0]
	requireEqual(t, string(j.Body), `{"id":42}`, "body")
	requireEqual(t, j.Attributes["kind"], "order", "attribute kind")
	requireEqual(t, j.Attributes["ApproximateReceiveCount"], "1", "ApproximateReceiveCount")
	if j.ID == "" || j.ReceiptHandle == "" {
		t.Fatalf("job without an ID or a receipt handle: %+v", j)
	}

	requireNoErr(t, q.Ack(t.Context(), "orders", j), "ack")
	requireCounts(t, c, u, counts{}, "after the ack")
}

func TestGasQueueNack(t *testing.T) {
	c, srv := newClient(t)
	u := createQueue(t, c, "retry", nil)
	q := gasQueue(t, srv, 0)

	requireNoErr(t, q.Enqueue(t.Context(), "retry", []byte("again")), "enqueue")
	jobs, err := q.Dequeue(t.Context(), "retry", 1, 0)
	requireNoErr(t, err, "dequeue")
	requireEqual(t, len(jobs), 1, "jobs")
	requireCounts(t, c, u, counts{notVisible: 1}, "while leased")

	requireNoErr(t, q.Nack(t.Context(), "retry", jobs[0]), "nack")
	again, err := q.Dequeue(t.Context(), "retry", 1, 0)
	requireNoErr(t, err, "dequeue after the nack")
	requireEqual(t, len(again), 1, "jobs after the nack")
	requireEqual(t, again[0].ID, jobs[0].ID, "the same message")
	requireEqual(t, again[0].Attributes["ApproximateReceiveCount"], "2", "ApproximateReceiveCount")

	// "MessageNotInflight wraps queue.ErrNotInFlight": SQS documents a
	// change of visibility of a message whose lease has ended.
	requireNoErr(t, q.NackAfter(t.Context(), "retry", again[0], time.Second), "nack after a second")
	time.Sleep(1500 * time.Millisecond)
	if err := q.Nack(t.Context(), "retry", again[0]); !errors.Is(err, queue.ErrNotInFlight) {
		t.Fatalf("nack of a message whose lease ended: %v, want queue.ErrNotInFlight", err)
	}
}

func TestGasQueueExtend(t *testing.T) {
	c, srv := newClient(t)
	u := createQueue(t, c, "slow", nil)
	q := gasQueue(t, srv, time.Second)

	requireNoErr(t, q.Enqueue(t.Context(), "slow", []byte("long job")), "enqueue")
	jobs, err := q.Dequeue(t.Context(), "slow", 1, 0)
	requireNoErr(t, err, "dequeue")
	requireEqual(t, len(jobs), 1, "jobs")

	requireNoErr(t, q.Extend(t.Context(), "slow", jobs[0], 30*time.Second), "extend")
	time.Sleep(1500 * time.Millisecond)
	none, err := q.Dequeue(t.Context(), "slow", 1, 0)
	requireNoErr(t, err, "dequeue past the first lease")
	requireEqual(t, len(none), 0, "jobs while extended")
	requireCounts(t, c, u, counts{notVisible: 1}, "while extended")
	requireNoErr(t, q.Ack(t.Context(), "slow", jobs[0]), "ack")
}

func TestGasQueueShortPollOnAWaitingQueue(t *testing.T) {
	// The store sends "WaitTimeSeconds":0 so a queue whose
	// ReceiveMessageWaitTimeSeconds is set still answers a short poll at
	// once (package documentation, Receiving).
	c, srv := newClient(t)
	createQueue(t, c, "patient", map[string]string{"ReceiveMessageWaitTimeSeconds": "10"})
	q := gasQueue(t, srv, 0)

	within(t, 3*time.Second, "a short poll on a queue that waits 10s", func() {
		jobs, err := q.Dequeue(t.Context(), "patient", 1, 0)
		requireNoErr(t, err, "dequeue")
		requireEqual(t, len(jobs), 0, "jobs")
	})
}

func TestGasQueueFIFO(t *testing.T) {
	c, srv := newClient(t)
	createFIFO(t, c, "jobs.fifo", nil)
	q := gasQueue(t, srv, 0)

	for _, b := range []string{"first", "second"} {
		requireNoErr(t, q.Enqueue(t.Context(), "jobs.fifo", []byte(b),
			gas.WithGroupID("tenant-1"), gas.WithDedupeID("id-"+b)), "enqueue "+b)
	}
	requireNoErr(t, q.Enqueue(t.Context(), "jobs.fifo", []byte("first"),
		gas.WithGroupID("tenant-1"), gas.WithDedupeID("id-first")), "enqueue a duplicate")

	jobs, err := q.Dequeue(t.Context(), "jobs.fifo", 10, time.Second)
	requireNoErr(t, err, "dequeue")
	got := make([]string, 0, len(jobs))
	for _, j := range jobs {
		got = append(got, string(j.Body))
	}
	requireSlice(t, got, []string{"first", "second"}, "bodies in order, deduplicated")

	// A store resolves names with GetQueueUrl; the client it exposes is
	// the SDK's, for operations it has no method for.
	_, err = q.Client().
		PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(srv.queueURL("jobs.fifo"))})
	requireNoErr(t, err, "purge through the gas client")
}

func TestGasQueueMissingQueue(t *testing.T) {
	_, srv := newClient(t)
	q := gasQueue(t, srv, 0)
	// "queue does not exist: QueueDoesNotExist, also wrapping
	// queue.ErrQueueNotFound" (Error replies).
	if err := q.Enqueue(
		t.Context(),
		"absent",
		[]byte("x"),
	); !errors.Is(
		err,
		queue.ErrQueueNotFound,
	) {
		t.Fatalf("enqueue to a missing queue: %v, want queue.ErrQueueNotFound", err)
	}
}
