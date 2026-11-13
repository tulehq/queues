//go:build e2e

package e2e

import (
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// FIFO queues against CreateQueue's FIFO attributes, SendMessage's
// MessageGroupId and MessageDeduplicationId, ReceiveMessage's
// ReceiveRequestAttemptId, and the SQS Developer Guide's FIFO pages as the
// SDK documentation quotes them.

func TestFIFONamingRule(t *testing.T) {
	// "A FIFO queue name must end with the .fifo suffix" and a standard
	// name is alphanumerics, hyphens and underscores (CreateQueue,
	// QueueName). Which code SQS answers either mismatch with is not
	// documented; a 400 is asserted.
	c, srv := newClient(t)

	u := createFIFO(t, c, "orders.fifo", nil)
	requireEqual(t, u, srv.queueURL("orders.fifo"), "FIFO queue URL")
	requireEqual(t, queueAttrs(t, c, u, "FifoQueue")["FifoQueue"], "true", "FifoQueue")

	requireRefused(t, errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName:  aws.String("no-suffix"),
		Attributes: map[string]string{"FifoQueue": "true"},
	})))
	requireQueueMissing(t, c, "no-suffix")

	requireRefused(
		t,
		errOf(
			c.CreateQueue(
				t.Context(),
				&sqs.CreateQueueInput{QueueName: aws.String("standard.fifo")},
			),
		),
	)
	requireQueueMissing(t, c, "standard.fifo")
}

func TestFIFOAttributes(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "attrs.fifo", nil)
	a := queueAttrs(t, c, u, "All")
	requireEqual(t, a["FifoQueue"], "true", "FifoQueue")
	requireEqual(t, a["ContentBasedDeduplication"], "false", "ContentBasedDeduplication")
	requireEqual(t, a["QueueArn"], "arn:aws:sqs:us-east-1:000000000000:attrs.fifo", "QueueArn")

	hi := createFIFO(t, c, "high.fifo", map[string]string{
		"DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId",
	})
	a = queueAttrs(t, c, hi, "DeduplicationScope", "FifoThroughputLimit")
	requireEqual(t, a["DeduplicationScope"], "messageGroup", "DeduplicationScope")
	requireEqual(t, a["FifoThroughputLimit"], "perMessageGroupId", "FifoThroughputLimit")

	// Valid values are messageGroup and queue, perQueue and
	// perMessageGroupId (CreateQueue).
	for k, v := range map[string]string{"DeduplicationScope": "both", "FifoThroughputLimit": "fast"} {
		err := errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
			QueueName:  aws.String("bad-" + k + ".fifo"),
			Attributes: map[string]string{"FifoQueue": "true", k: v},
		}))
		requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
	}
}

func TestFIFOSendRules(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "rules.fifo", nil)

	t.Run("MessageGroupId is required", func(t *testing.T) {
		// "If you do not provide a MessageGroupId when sending a message to
		// a FIFO queue, the action fails." Assumption: as a missing
		// required parameter, MissingParameter (Common Errors).
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("x"),
			MessageDeduplicationId: aws.String("d"),
		}))
		requireCode(t, err, "MissingParameter")
	})

	t.Run("a deduplication ID is required without content-based deduplication", func(t *testing.T) {
		// "the action fails with an error" (MessageDeduplicationId); Gas's
		// queue/sqs documents SQS's code as InvalidParameterValue.
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("x"), MessageGroupId: aws.String("g"),
		}))
		requireInvalidParameter(t, err)
	})

	t.Run("per-message DelaySeconds is refused", func(t *testing.T) {
		// "When you set FifoQueue, you can't set DelaySeconds per message"
		// (SendMessage). Assumption: InvalidParameterValue.
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("x"), MessageGroupId: aws.String("g"),
			MessageDeduplicationId: aws.String("d"), DelaySeconds: 5,
		}))
		requireInvalidParameter(t, err)
	})

	requireCounts(t, c, u, counts{}, "nothing stored by refused sends")
}

func TestFIFODeduplicationByID(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "dedup.fifo", nil)

	first := sendFIFO(t, c, u, "one", "g", "same-id")
	// "any messages sent with the same MessageDeduplicationId are accepted
	// successfully but aren't delivered during the 5-minute deduplication
	// interval".
	sendFIFO(t, c, u, "two", "g", "same-id")
	sendFIFO(t, c, u, "three", "g", "other-id")

	msgs := receive(t, c, u, 10)
	requireSlice(t, bodies(msgs), []string{"one", "three"}, "delivered bodies")
	requireEqual(
		t,
		aws.ToString(msgs[0].MessageId),
		aws.ToString(first.MessageId),
		"first MessageId",
	)
	for _, m := range msgs {
		deleteMessage(t, c, u, m)
	}

	// "Amazon SQS continues to keep track of the message deduplication ID
	// even after the message is received and deleted."
	sendFIFO(t, c, u, "four", "g", "same-id")
	requireEmpty(t, c, u, "a deleted message's deduplication ID sent again")
}

func TestFIFOContentBasedDeduplication(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "content.fifo", map[string]string{"ContentBasedDeduplication": "true"})
	requireEqual(t, queueAttrs(t, c, u, "ContentBasedDeduplication")["ContentBasedDeduplication"],
		"true", "ContentBasedDeduplication")

	// "uses a SHA-256 hash to generate the MessageDeduplicationId using the
	// body of the message (but not the attributes of the message)".
	sendFIFO(t, c, u, "same body", "g", "")
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(
			u,
		),
		MessageBody:    aws.String("same body"),
		MessageGroupId: aws.String("g"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"a": stringAttr("String", "differs"),
		},
	})
	sendFIFO(t, c, u, "other body", "g", "")
	// "your MessageDeduplicationId overrides the generated one."
	sendFIFO(t, c, u, "same body", "g", "explicit")

	requireSlice(t, bodies(receive(t, c, u, 10)), []string{"same body", "other body", "same body"},
		"delivered bodies")
}

func TestFIFODeduplicationScope(t *testing.T) {
	c, _ := newClient(t)

	// DeduplicationScope "Specifies whether message deduplication occurs at
	// the message group or queue level"; queue is the default.
	q := createFIFO(t, c, "per-queue.fifo", nil)
	sendFIFO(t, c, q, "a", "g1", "dup")
	sendFIFO(t, c, q, "b", "g2", "dup")
	requireSlice(t, bodies(receive(t, c, q, 10)), []string{"a"}, "per-queue scope")

	g := createFIFO(t, c, "per-group.fifo", map[string]string{"DeduplicationScope": "messageGroup"})
	sendFIFO(t, c, g, "a", "g1", "dup")
	sendFIFO(t, c, g, "b", "g2", "dup")
	sendFIFO(t, c, g, "c", "g2", "dup")
	requireSameSet(t, bodies(receive(t, c, g, 10)), []string{"a", "b"}, "per-group scope")
}

func TestFIFODeduplicationWindow(t *testing.T) {
	srv, clock := startServerWithClock(t)
	c := clientFor(srv)
	u := createFIFO(t, c, "window.fifo", nil)

	sendFIFO(t, c, u, "first", "g", "d")
	deleteMessage(t, c, u, receiveOne(t, c, u))

	clock.Advance(t, 4*time.Minute+50*time.Second)
	sendFIFO(t, c, u, "inside", "g", "d")
	requireEmpty(t, c, u, "a duplicate within 5 minutes")

	// Assumption: the interval counts from the first send, so a send past
	// 5 minutes of it is new.
	clock.Advance(t, 20*time.Second)
	sendFIFO(t, c, u, "after", "g", "d")
	requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), "after", "body sent after the window")
}

func TestFIFOOrderingWithinAGroup(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "ordered.fifo", nil)
	a, b := make([]string, 0, 4), make([]string, 0, 4)
	for i := range 4 {
		sendFIFO(t, c, u, "A"+strconv.Itoa(i), "A", "a"+strconv.Itoa(i))
		sendFIFO(t, c, u, "B"+strconv.Itoa(i), "B", "b"+strconv.Itoa(i))
		a = append(a, "A"+strconv.Itoa(i))
		b = append(b, "B"+strconv.Itoa(i))
	}

	// "For each MessageGroupId, the messages are sorted by time sent."
	var gotA, gotB []string
	for range 10 {
		msgs := receive(t, c, u, 10)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			switch m.Attributes["MessageGroupId"] {
			case "A":
				gotA = append(gotA, aws.ToString(m.Body))
			case "B":
				gotB = append(gotB, aws.ToString(m.Body))
			default:
				t.Fatalf(
					"message %q has MessageGroupId %q",
					aws.ToString(m.Body),
					m.Attributes["MessageGroupId"],
				)
			}
			deleteMessage(t, c, u, m)
		}
	}
	requireSlice(t, gotA, a, "group A order")
	requireSlice(t, gotB, b, "group B order")
}

func TestFIFOBatchKeepsOrder(t *testing.T) {
	// "For a FIFO queue, multiple messages within a single batch are
	// enqueued in the order they are sent" (SendMessageBatch).
	c, _ := newClient(t)
	u := createFIFO(t, c, "batch.fifo", nil)
	entries := make([]types.SendMessageBatchRequestEntry, 0, 5)
	want := make([]string, 0, 5)
	for i := range 5 {
		s := strconv.Itoa(i)
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id: aws.String("e" + s), MessageBody: aws.String("m" + s),
			MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("d" + s),
		})
		want = append(want, "m"+s)
	}
	out, err := c.SendMessageBatch(
		t.Context(),
		&sqs.SendMessageBatchInput{QueueUrl: aws.String(u), Entries: entries},
	)
	requireNoErr(t, err, "send message batch")
	requireEqual(t, len(out.Successful), 5, "successful entries")
	for _, s := range out.Successful {
		if aws.ToString(s.SequenceNumber) == "" {
			t.Fatalf("batch entry %s has no SequenceNumber", aws.ToString(s.Id))
		}
	}
	requireSlice(t, bodies(receive(t, c, u, 10)), want, "bodies in order")
}

func TestFIFOGroupHeldWhileInFlight(t *testing.T) {
	// "While messages with a particular MessageGroupId are invisible, no
	// more messages belonging to the same MessageGroupId are returned until
	// the visibility timeout expires. You can still receive messages with
	// another MessageGroupId" (ReceiveRequestAttemptId).
	c, _ := newClient(t)
	u := createFIFO(t, c, "held.fifo", nil)
	sendFIFO(t, c, u, "A1", "A", "1")
	sendFIFO(t, c, u, "A2", "A", "2")
	sendFIFO(t, c, u, "B1", "B", "3")

	first := receiveOne(t, c, u)
	held := first.Attributes["MessageGroupId"]
	others := receive(t, c, u, 10)
	for _, m := range others {
		if m.Attributes["MessageGroupId"] == held {
			t.Fatalf("received %q of group %s while %q of it is in flight",
				aws.ToString(m.Body), held, aws.ToString(first.Body))
		}
	}
	requireEqual(t, len(others), 1, "messages of the other group")
	requireEmpty(t, c, u, "every group held")

	if held == "A" {
		requireEqual(t, aws.ToString(first.Body), "A1", "first of group A")
		deleteMessage(t, c, u, first)
		requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), "A2", "next of group A")
		return
	}
	requireEqual(t, aws.ToString(first.Body), "B1", "first of group B")
}

func TestFIFOGroupReleasedByVisibility(t *testing.T) {
	c, _ := newClient(t)
	u := createFIFO(t, c, "released.fifo", nil)
	sendFIFO(t, c, u, "A1", "A", "1")
	sendFIFO(t, c, u, "A2", "A", "2")

	m := receiveOne(t, c, u)
	requireEqual(t, aws.ToString(m.Body), "A1", "first")
	requireEmpty(t, c, u, "the group held")
	changeVisibility(t, c, u, m, 0)
	// The failed message comes back first: order within the group holds.
	again := receive(t, c, u, 10)
	requireSlice(t, bodies(again), []string{"A1", "A2"}, "after the release")
}

func TestFIFOSequenceNumber(t *testing.T) {
	// SequenceNumber: "The large, non-consecutive number that Amazon SQS
	// assigns to each message ... continues to increase for a particular
	// MessageGroupId" (SendMessage); returned on receive (SequenceNumber,
	// MessageGroupId and MessageDeduplicationId system attributes).
	c, _ := newClient(t)
	u := createFIFO(t, c, "seq.fifo", nil)

	seqs := make([]string, 0, 3)
	for i := range 3 {
		out := sendFIFO(t, c, u, "m"+strconv.Itoa(i), "g", "d"+strconv.Itoa(i))
		seqs = append(seqs, aws.ToString(out.SequenceNumber))
	}
	prev := new(big.Int)
	for i, s := range seqs {
		n, ok := new(big.Int).SetString(s, 10)
		if !ok {
			t.Fatalf("SequenceNumber %q is not a decimal number", s)
		}
		if i > 0 && n.Cmp(prev) <= 0 {
			t.Fatalf("SequenceNumber %s does not increase from %s", s, prev)
		}
		prev = n
	}

	msgs := receive(t, c, u, 10)
	requireEqual(t, len(msgs), 3, "received")
	for i, m := range msgs {
		requireEqual(t, m.Attributes["SequenceNumber"], seqs[i], "SequenceNumber attribute")
		requireEqual(t, m.Attributes["MessageGroupId"], "g", "MessageGroupId attribute")
		requireEqual(t, m.Attributes["MessageDeduplicationId"], "d"+strconv.Itoa(i),
			"MessageDeduplicationId attribute")
	}
}

func TestFIFOReceiveRequestAttemptID(t *testing.T) {
	// "During a visibility timeout, subsequent calls with the same
	// ReceiveRequestAttemptId return the same messages and receipt handles."
	c, _ := newClient(t)
	u := createFIFO(t, c, "attempt.fifo", nil)
	sendFIFO(t, c, u, "A1", "A", "1")
	sendFIFO(t, c, u, "B1", "B", "2")

	in := func() *sqs.ReceiveMessageInput {
		return &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(u), MaxNumberOfMessages: 10,
			ReceiveRequestAttemptId: aws.String("attempt-1"),
		}
	}
	first := receiveInput(t, c, in())
	retry := receiveInput(t, c, in())
	requireEqual(t, len(first), 2, "first attempt")
	requireEqual(t, len(retry), len(first), "retried attempt")
	for i := range first {
		requireEqual(
			t,
			aws.ToString(retry[i].MessageId),
			aws.ToString(first[i].MessageId),
			"MessageId",
		)
		requireEqual(t, aws.ToString(retry[i].ReceiptHandle), aws.ToString(first[i].ReceiptHandle),
			"ReceiptHandle")
	}

	// Another attempt ID gets nothing: both groups are in flight.
	other := receiveInput(t, c, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(
			u,
		),
		MaxNumberOfMessages:     10,
		ReceiveRequestAttemptId: aws.String("attempt-2"),
	})
	requireEqual(t, len(other), 0, "a different attempt")
}
