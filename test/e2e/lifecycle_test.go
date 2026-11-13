//go:build e2e

package e2e

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Deleting, visibility, delay, retention and redrive, against
// api_op_DeleteMessage*.go, api_op_ChangeMessageVisibility*.go, the queue
// attributes of api_op_CreateQueue.go and types/errors.go.

func TestDeleteMessage(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "deleted", map[string]string{"VisibilityTimeout": "1"})
	send(t, c, u, "gone")
	m := receiveOne(t, c, u)

	deleteMessage(t, c, u, m)
	requireCounts(t, c, u, counts{}, "after the delete")
	// Past the visibility timeout the message does not come back.
	msgs := receiveInput(
		t,
		c,
		&sqs.ReceiveMessageInput{QueueUrl: aws.String(u), WaitTimeSeconds: 2},
	)
	requireEqual(t, len(msgs), 0, "messages after the delete")

	// "The specified receipt handle isn't valid" (ReceiptHandleIsInvalid).
	err := errOf(c.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
		QueueUrl: aws.String(u), ReceiptHandle: aws.String("not-a-receipt-handle"),
	}))
	requireModeled[*types.ReceiptHandleIsInvalid](t, err, "ReceiptHandleIsInvalid")

	requireNonExistent(t, errOf(c.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
		QueueUrl: aws.String(u + "-missing"), ReceiptHandle: m.ReceiptHandle,
	})))
}

func TestDeleteMessageWhileHiddenByAnotherReceive(t *testing.T) {
	// "Amazon SQS can delete a message from a queue even if a visibility
	// timeout setting causes the message to be locked by another consumer"
	// (DeleteMessage): the latest receipt handle deletes it.
	c, _ := newClient(t)
	u := createQueue(t, c, "latest", nil)
	send(t, c, u, "x")
	first := receiveOne(t, c, u)
	changeVisibility(t, c, u, first, 0)
	second := receiveOne(t, c, u)
	deleteMessage(t, c, u, second)
	requireCounts(t, c, u, counts{}, "after deleting with the latest handle")
}

func TestChangeMessageVisibility(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "vis", nil)

	t.Run("0 makes the message visible at once", func(t *testing.T) {
		send(t, c, u, "nack")
		m := receiveOne(t, c, u)
		changeVisibility(t, c, u, m, 0)
		again := receiveOne(t, c, u)
		requireEqual(t, aws.ToString(again.MessageId), aws.ToString(m.MessageId), "MessageId")
		requireEqual(t, receiveCount(t, again), 2, "receive count")
		deleteMessage(t, c, u, again)
	})

	t.Run("a longer timeout keeps it hidden", func(t *testing.T) {
		send(t, c, u, "extend")
		m := receiveInput(
			t,
			c,
			&sqs.ReceiveMessageInput{QueueUrl: aws.String(u), VisibilityTimeout: 1},
		)
		requireEqual(t, len(m), 1, "received")
		changeVisibility(t, c, u, m[0], 30)
		time.Sleep(1500 * time.Millisecond)
		requireEmpty(t, c, u, "past the receive's timeout, within the new one")
		requireCounts(t, c, u, counts{notVisible: 1}, "while extended")
		deleteMessage(t, c, u, m[0])
	})

	t.Run("a shorter timeout counts from the change", func(t *testing.T) {
		send(t, c, u, "shorten")
		m := receiveOne(t, c, u) // the queue's 30 seconds
		changeVisibility(t, c, u, m, 1)
		msgs := receiveInput(
			t,
			c,
			&sqs.ReceiveMessageInput{QueueUrl: aws.String(u), WaitTimeSeconds: 5},
		)
		requireEqual(t, len(msgs), 1, "visible a second after the change")
		deleteMessage(t, c, u, msgs[0])
	})

	t.Run("above 12 hours is InvalidParameterValue", func(t *testing.T) {
		send(t, c, u, "too long")
		m := receiveOne(t, c, u)
		requireInvalidParameter(
			t,
			errOf(c.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{
				QueueUrl: aws.String(u), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 43201,
			})),
		)
		changeVisibility(t, c, u, m, 43000) // within the 12 hours left since the receive
		deleteMessage(t, c, u, m)
	})

	t.Run("an invalid handle is ReceiptHandleIsInvalid", func(t *testing.T) {
		err := errOf(c.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(u), ReceiptHandle: aws.String("bogus"), VisibilityTimeout: 5,
		}))
		requireModeled[*types.ReceiptHandleIsInvalid](t, err, "ReceiptHandleIsInvalid")
	})
}

func TestChangeVisibilityOfAMessageNotInFlight(t *testing.T) {
	// "The specified message isn't in flight" (MessageNotInflight): its
	// visibility timeout ended and nobody received it since.
	c, _ := newClient(t)
	u := createQueue(t, c, "expired", nil)
	send(t, c, u, "x")
	m := receiveInput(t, c, &sqs.ReceiveMessageInput{QueueUrl: aws.String(u), VisibilityTimeout: 1})
	requireEqual(t, len(m), 1, "received")
	time.Sleep(1500 * time.Millisecond)
	requireCounts(t, c, u, counts{visible: 1}, "after the timeout")

	err := errOf(c.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(u), ReceiptHandle: m[0].ReceiptHandle, VisibilityTimeout: 30,
	}))
	requireModeled[*types.MessageNotInflight](t, err, "AWS.SimpleQueueService.MessageNotInflight")
}

func TestVisibilityExpiryRedelivers(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "redelivered", map[string]string{"VisibilityTimeout": "1"})
	send(t, c, u, "again and again")

	var handles []string
	for want := 1; want <= 3; want++ {
		msgs := receiveInput(t, c, &sqs.ReceiveMessageInput{
			QueueUrl:        aws.String(u),
			WaitTimeSeconds: 5,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				"ApproximateReceiveCount",
			},
		})
		requireEqual(t, len(msgs), 1, "receive "+strconv.Itoa(want))
		requireEqual(t, receiveCount(t, msgs[0]), want, "ApproximateReceiveCount")
		handles = append(handles, aws.ToString(msgs[0].ReceiptHandle))
	}
	if handles[0] == handles[1] || handles[1] == handles[2] {
		t.Fatalf("receipt handles repeat across receives: %q", handles)
	}
}

// batchMessages sends n messages and receives them all.
func batchMessages(t *testing.T, c *sqs.Client, u string, n int) []types.Message {
	t.Helper()
	for i := range n {
		send(t, c, u, "b"+strconv.Itoa(i))
	}
	msgs := receive(t, c, u, int32(n)) //nolint:gosec // G115: small n
	requireEqual(t, len(msgs), n, "received for the batch")
	return msgs
}

func TestDeleteMessageBatch(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "del-batch", nil)
	msgs := batchMessages(t, c, u, 3)

	out, err := c.DeleteMessageBatch(t.Context(), &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(u),
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("a"), ReceiptHandle: msgs[0].ReceiptHandle},
			{Id: aws.String("b"), ReceiptHandle: msgs[1].ReceiptHandle},
			{Id: aws.String("bad"), ReceiptHandle: aws.String("bogus")},
		},
	})
	requireNoErr(t, err, "delete message batch")
	ok := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		ok = append(ok, aws.ToString(s.Id))
	}
	requireSameSet(t, ok, []string{"a", "b"}, "deleted entries")
	requireBatchFailed(t, out.Failed, "bad")
	requireCounts(t, c, u, counts{notVisible: 1}, "after the batch")

	entry := func(id string) types.DeleteMessageBatchRequestEntry {
		return types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(id),
			ReceiptHandle: msgs[2].ReceiptHandle,
		}
	}
	del := func(es ...types.DeleteMessageBatchRequestEntry) error {
		return errOf(c.DeleteMessageBatch(t.Context(), &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(u), Entries: es,
		}, skipValidation))
	}
	eleven := make([]types.DeleteMessageBatchRequestEntry, 0, 11)
	for i := range 11 {
		eleven = append(eleven, entry("e"+strconv.Itoa(i)))
	}
	requireModeled[*types.TooManyEntriesInBatchRequest](t, del(eleven...),
		"AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	requireModeled[*types.EmptyBatchRequest](t, del(), "AWS.SimpleQueueService.EmptyBatchRequest")
	requireModeled[*types.BatchEntryIdsNotDistinct](t, del(entry("x"), entry("x")),
		"AWS.SimpleQueueService.BatchEntryIdsNotDistinct")
	requireModeled[*types.InvalidBatchEntryId](t, del(entry("not ok")),
		"AWS.SimpleQueueService.InvalidBatchEntryId")
	requireNonExistent(t, errOf(c.DeleteMessageBatch(t.Context(), &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(
			u + "-missing",
		),
		Entries: []types.DeleteMessageBatchRequestEntry{entry("x")},
	})))
	requireCounts(t, c, u, counts{notVisible: 1}, "refused batches deleted nothing")
}

func TestChangeMessageVisibilityBatch(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "vis-batch", nil)
	msgs := batchMessages(t, c, u, 2)

	out, err := c.ChangeMessageVisibilityBatch(t.Context(), &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(u),
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{Id: aws.String("now"), ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 0},
			{Id: aws.String("later"), ReceiptHandle: msgs[1].ReceiptHandle, VisibilityTimeout: 600},
			{Id: aws.String("bad"), ReceiptHandle: aws.String("bogus"), VisibilityTimeout: 0},
		},
	})
	requireNoErr(t, err, "change message visibility batch")
	ok := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		ok = append(ok, aws.ToString(s.Id))
	}
	requireSameSet(t, ok, []string{"now", "later"}, "changed entries")
	requireBatchFailed(t, out.Failed, "bad")
	requireCounts(t, c, u, counts{visible: 1, notVisible: 1}, "after the batch")
	requireEqual(t, aws.ToString(receiveOne(t, c, u).MessageId), aws.ToString(msgs[0].MessageId),
		"the message made visible")

	entry := func(id string) types.ChangeMessageVisibilityBatchRequestEntry {
		return types.ChangeMessageVisibilityBatchRequestEntry{
			Id: aws.String(id), ReceiptHandle: msgs[1].ReceiptHandle, VisibilityTimeout: 5,
		}
	}
	change := func(es ...types.ChangeMessageVisibilityBatchRequestEntry) error {
		return errOf(
			c.ChangeMessageVisibilityBatch(t.Context(), &sqs.ChangeMessageVisibilityBatchInput{
				QueueUrl: aws.String(u), Entries: es,
			}, skipValidation),
		)
	}
	eleven := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, 11)
	for i := range 11 {
		eleven = append(eleven, entry("e"+strconv.Itoa(i)))
	}
	requireModeled[*types.TooManyEntriesInBatchRequest](t, change(eleven...),
		"AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	requireModeled[*types.EmptyBatchRequest](
		t,
		change(),
		"AWS.SimpleQueueService.EmptyBatchRequest",
	)
	requireModeled[*types.BatchEntryIdsNotDistinct](t, change(entry("x"), entry("x")),
		"AWS.SimpleQueueService.BatchEntryIdsNotDistinct")
	requireModeled[*types.InvalidBatchEntryId](t, change(entry("not ok")),
		"AWS.SimpleQueueService.InvalidBatchEntryId")
}

func TestDelay(t *testing.T) {
	c, _ := newClient(t)

	t.Run("per message", func(t *testing.T) {
		u := createQueue(t, c, "delayed-msg", nil)
		sendInput(t, c, &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("later"), DelaySeconds: 1,
		})
		requireCounts(t, c, u, counts{delayed: 1}, "while delayed")
		requireEmpty(t, c, u, "receive while delayed")
		requireDelivered(t, c, u, "later")
	})

	t.Run("by the queue's DelaySeconds", func(t *testing.T) {
		u := createQueue(t, c, "delay-queue", map[string]string{"DelaySeconds": "1"})
		send(t, c, u, "queued later")
		requireCounts(t, c, u, counts{delayed: 1}, "while delayed")
		requireEmpty(t, c, u, "receive while delayed")
		requireDelivered(t, c, u, "queued later")
	})
}

// requireDelivered long-polls up to 5 seconds for one message with body.
func requireDelivered(t *testing.T, c *sqs.Client, u, body string) {
	t.Helper()
	msgs := receiveInput(
		t,
		c,
		&sqs.ReceiveMessageInput{QueueUrl: aws.String(u), WaitTimeSeconds: 5},
	)
	requireSlice(t, bodies(msgs), []string{body}, "bodies after the delay")
}

func TestDelayOnTheServiceClock(t *testing.T) {
	srv, clock := startServerWithClock(t)
	c := clientFor(srv)
	u := createQueue(t, c, "clock-delay", nil)
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("fifteen minutes"), DelaySeconds: 900,
	})
	clock.Advance(t, 899*time.Second)
	requireEmpty(t, c, u, "a second before the delay ends")
	clock.Advance(t, 2*time.Second)
	requireEqual(
		t,
		aws.ToString(receiveOne(t, c, u).Body),
		"fifteen minutes",
		"body after the delay",
	)
}

func TestRetention(t *testing.T) {
	// MessageRetentionPeriod: "The length of time, in seconds, for which
	// Amazon SQS retains a message", at least 60.
	srv, clock := startServerWithClock(t)
	c := clientFor(srv)
	u := createQueue(t, c, "retained", map[string]string{"MessageRetentionPeriod": "60"})
	send(t, c, u, "expires")
	send(t, c, u, "in flight expires too")
	receiveOne(t, c, u)

	clock.Advance(t, 30*time.Second)
	send(t, c, u, "younger")
	clock.Advance(t, 31*time.Second)

	requireCounts(t, c, u, counts{visible: 1}, "after the first two outlived retention")
	requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), "younger", "the surviving body")
}

func TestVisibilityOnTheServiceClock(t *testing.T) {
	srv, clock := startServerWithClock(t)
	c := clientFor(srv)
	u := createQueue(t, c, "clock-vis", nil) // VisibilityTimeout 30
	send(t, c, u, "x")
	receiveOne(t, c, u)
	clock.Advance(t, 29*time.Second)
	requireEmpty(t, c, u, "before the timeout")
	clock.Advance(t, 2*time.Second)
	requireEqual(t, receiveCount(t, receiveOne(t, c, u)), 2, "receive count after the timeout")
}

func TestRedriveToDeadLetterQueue(t *testing.T) {
	// "When the ReceiveCount for a message exceeds the maxReceiveCount for
	// a queue, Amazon SQS moves the message to the dead-letter-queue"
	// (CreateQueue, RedrivePolicy).
	c, _ := newClient(t)
	dlq := createQueue(t, c, "dead", nil)
	dlqARN := queueAttrs(t, c, dlq, "QueueArn")["QueueArn"]
	src := createQueue(t, c, "work", map[string]string{"RedrivePolicy": redrivePolicy(dlqARN, 2)})
	sent := send(t, c, src, "poison")

	for i := 1; i <= 2; i++ {
		m := receiveOne(t, c, src)
		requireEqual(t, receiveCount(t, m), i, "receive count of the poison message")
		changeVisibility(t, c, src, m, 0)
	}

	// The third receive would exceed maxReceiveCount 2: the message moves.
	requireEmpty(t, c, src, "the source after maxReceiveCount receives")
	requireCounts(t, c, src, counts{}, "the source")
	requireCounts(t, c, dlq, counts{visible: 1}, "dead-letter queue")
	moved := receiveOne(t, c, dlq)
	requireEqual(
		t,
		aws.ToString(moved.MessageId),
		aws.ToString(sent.MessageId),
		"MessageId in the DLQ",
	)
	requireEqual(t, aws.ToString(moved.Body), "poison", "body in the DLQ")
	// Assumption: DeadLetterQueueSourceArn, a valid MessageSystemAttributeNames
	// value in the API reference whose meaning it does not spell out, names
	// the source queue, as on SQS.
	requireEqual(t, moved.Attributes["DeadLetterQueueSourceArn"],
		queueAttrs(t, c, src, "QueueArn")["QueueArn"], "DeadLetterQueueSourceArn")
}
