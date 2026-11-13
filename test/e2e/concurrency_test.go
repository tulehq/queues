//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Concurrent receivers. A received message is hidden from every other
// receiver for its visibility timeout (ReceiveMessage, VisibilityTimeout:
// "it becomes temporarily invisible to other consumers"), and a FIFO group
// is processed by one consumer at a time in order (SendMessage,
// MessageGroupId: "Messages within the same message group are always
// processed one at a time, in strict order"; Developer Guide, FIFO queue
// delivery logic).

const (
	receivers       = 8
	concurrentBound = 30 * time.Second
)

// runReceivers runs n receivers until done reports true or the bound
// passes, each calling handle with what one ReceiveMessage returned. It
// fails the test on the first error any of them reports.
func runReceivers(
	t *testing.T,
	n int,
	receiveIn func() *sqs.ReceiveMessageInput,
	c *sqs.Client,
	handle func(ctx context.Context, worker int, msgs []types.Message) error,
	done func() bool,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), concurrentBound)
	defer cancel()

	errs := make(chan error, n)
	var wg sync.WaitGroup
	for w := range n {
		wg.Go(func() {
			for ctx.Err() == nil && !done() {
				out, err := c.ReceiveMessage(ctx, receiveIn())
				if err != nil {
					if ctx.Err() == nil {
						errs <- fmt.Errorf("receiver %d: %w", w, err)
						cancel()
					}
					return
				}
				if err := handle(ctx, w, out.Messages); err != nil {
					errs <- fmt.Errorf("receiver %d: %w", w, err)
					cancel()
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if !done() {
		t.Fatalf("the receivers did not finish within %v", concurrentBound)
	}
}

func TestConcurrentReceiversGetNoDuplicates(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "shared", nil)
	const total = 60
	sent := make([]string, 0, total)
	for i := range total / 10 {
		entries := make([]types.SendMessageBatchRequestEntry, 0, 10)
		for j := range 10 {
			s := strconv.Itoa(i*10 + j)
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id: aws.String("e" + s), MessageBody: aws.String("m" + s),
			})
		}
		out, err := c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(u), Entries: entries,
		})
		requireNoErr(t, err, "send batch")
		requireEqual(t, len(out.Failed), 0, "failed entries")
		for _, s := range out.Successful {
			sent = append(sent, aws.ToString(s.MessageId))
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}
	got := 0
	runReceivers(t, receivers,
		func() *sqs.ReceiveMessageInput {
			return &sqs.ReceiveMessageInput{
				QueueUrl: aws.String(u), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
				VisibilityTimeout: 120,
			}
		}, c,
		func(ctx context.Context, w int, msgs []types.Message) error {
			mu.Lock()
			for _, m := range msgs {
				id := aws.ToString(m.MessageId)
				if prev, dup := seen[id]; dup {
					mu.Unlock()
					return fmt.Errorf("message %s already received by receiver %d", id, prev)
				}
				seen[id] = w
				got++
			}
			mu.Unlock()
			for _, m := range msgs {
				if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl: aws.String(u), ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					return fmt.Errorf("delete: %w", err)
				}
			}
			return nil
		},
		func() bool { mu.Lock(); defer mu.Unlock(); return got >= total },
	)
	requireSameSet(t, keys2(seen), sent, "every message received once")
}

func keys2(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// groupLedger tracks which receiver holds each FIFO group and the order
// each group's messages arrived in.
type groupLedger struct {
	holder map[string]int
	order  map[string][]int
	mu     sync.Mutex
	got    int
}

// acquire records that worker received msgs, failing if another receiver
// still holds one of their groups.
func (l *groupLedger) acquire(worker int, msgs []types.Message) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range msgs {
		g := m.Attributes["MessageGroupId"]
		if h, held := l.holder[g]; held && h != worker {
			return fmt.Errorf(
				"group %s handed to receiver %d while receiver %d holds it",
				g,
				worker,
				h,
			)
		}
		l.holder[g] = worker
		body := aws.ToString(m.Body)
		i, err := strconv.Atoi(body[strings.LastIndex(body, "-")+1:])
		if err != nil {
			return fmt.Errorf("unexpected body %q", body)
		}
		l.order[g] = append(l.order[g], i)
		l.got++
	}
	return nil
}

// release gives up worker's hold on the groups of msgs. The receiver calls
// it before it deletes them, while the service still holds the groups, so
// a hand-over can never be mistaken for a double hold.
func (l *groupLedger) release(msgs []types.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range msgs {
		delete(l.holder, m.Attributes["MessageGroupId"])
	}
}

func (l *groupLedger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.got
}

func TestConcurrentFIFOReceiversHoldGroupsApart(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "contended.fifo", nil)
	const groups, perGroup = 5, 12
	for i := range perGroup {
		for g := range groups {
			s := fmt.Sprintf("g%d-%d", g, i)
			sendFIFO(t, c, u, s, "g"+strconv.Itoa(g), s)
		}
	}

	ledger := &groupLedger{holder: map[string]int{}, order: map[string][]int{}}
	runReceivers(t, receivers,
		func() *sqs.ReceiveMessageInput {
			return &sqs.ReceiveMessageInput{
				QueueUrl: aws.String(u), MaxNumberOfMessages: 3, WaitTimeSeconds: 1,
				VisibilityTimeout:           120,
				MessageSystemAttributeNames: []types.MessageSystemAttributeName{"MessageGroupId"},
			}
		}, c,
		func(ctx context.Context, w int, msgs []types.Message) error {
			if len(msgs) == 0 {
				return nil
			}
			if err := ledger.acquire(w, msgs); err != nil {
				return err
			}
			time.Sleep(5 * time.Millisecond) // hold the group a little
			ledger.release(msgs)
			for _, m := range msgs {
				if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl: aws.String(u), ReceiptHandle: m.ReceiptHandle,
				}); err != nil {
					return errors.Join(errors.New("delete"), err)
				}
			}
			return nil
		},
		func() bool { return ledger.count() >= groups*perGroup },
	)

	want := make([]int, 0, perGroup)
	for i := range perGroup {
		want = append(want, i)
	}
	for g := range groups {
		name := "g" + strconv.Itoa(g)
		if !slices.Equal(ledger.order[name], want) {
			t.Fatalf("group %s arrived as %v, want %v", name, ledger.order[name], want)
		}
	}
}
