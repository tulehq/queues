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

// ReceiveMessage against api_op_ReceiveMessage.go and the API reference.

func TestReceiveMaxNumberOfMessages(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "many", nil)
	for i := range 12 {
		send(t, c, u, "m"+strconv.Itoa(i))
	}

	// "Default: 1" (MaxNumberOfMessages).
	out, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "receive with the default count")
	requireEqual(t, len(out.Messages), 1, "messages with the default count")

	// "Amazon SQS never returns more messages than this value". Assumption:
	// with no servers to sample, as many as are visible come back, so
	// exactly the count asked for.
	requireEqual(t, len(receive(t, c, u, 3)), 3, "messages with MaxNumberOfMessages 3")
	requireEqual(t, len(receive(t, c, u, 10)), 8, "messages with MaxNumberOfMessages 10")
	requireEmpty(t, c, u, "every message in flight")

	// Valid values 1 to 10; out of range is InvalidParameterValue (Common
	// Errors: "an invalid or out-of-range value").
	for _, n := range []int32{11, -1} {
		requireInvalidParameter(t, errOf(c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(u), MaxNumberOfMessages: n,
		})))
	}
	requireInvalidParameter(t, errOf(c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(u), WaitTimeSeconds: 21,
	})))
	requireInvalidParameter(t, errOf(c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(u), VisibilityTimeout: 43201,
	})))
	requireNonExistent(t, errOf(c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(u + "-missing"),
	})))
}

func TestReceiveVisibilityTimeout(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "hidden", nil)
	send(t, c, u, "once")

	first := receiveInput(t, c, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(u),
		VisibilityTimeout:           1,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{"ApproximateReceiveCount"},
	})
	requireEqual(t, len(first), 1, "first receive")
	requireEqual(t, receiveCount(t, first[0]), 1, "receive count of the first receive")
	requireEmpty(t, c, u, "receive while the message is hidden")
	requireCounts(t, c, u, counts{notVisible: 1}, "while hidden")

	// The request's VisibilityTimeout applies to the messages it returns;
	// once it ends the message is visible again with a new receipt handle
	// and its receive count raised.
	again := receiveInput(t, c, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(u),
		WaitTimeSeconds:             5,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{"ApproximateReceiveCount"},
	})
	requireEqual(t, len(again), 1, "receive after the timeout")
	requireEqual(t, aws.ToString(again[0].MessageId), aws.ToString(first[0].MessageId), "MessageId")
	requireEqual(t, receiveCount(t, again[0]), 2, "receive count after redelivery")
	if aws.ToString(again[0].ReceiptHandle) == aws.ToString(first[0].ReceiptHandle) {
		t.Fatal("redelivery reused the receipt handle; each receive gets a new one (DeleteMessage)")
	}
}

func TestReceiveUsesQueueVisibilityTimeout(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "queue-vt", map[string]string{"VisibilityTimeout": "1"})
	send(t, c, u, "x")
	receiveOne(t, c, u)
	requireEmpty(t, c, u, "within the queue's timeout")
	msgs := receiveInput(
		t,
		c,
		&sqs.ReceiveMessageInput{QueueUrl: aws.String(u), WaitTimeSeconds: 5},
	)
	requireEqual(t, len(msgs), 1, "after the queue's timeout")
}

func TestReceiveSystemAttributeSelection(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "selected", map[string]string{"VisibilityTimeout": "0"})
	sentAt := time.Now()
	send(t, c, u, "x")
	t.Run("none unless asked", func(t *testing.T) {
		requireEqual(t, len(recvAttrs(t, c, u, &sqs.ReceiveMessageInput{})), 0, "attribute count")
	})

	t.Run("named ones only", func(t *testing.T) {
		a := recvAttrs(t, c, u, &sqs.ReceiveMessageInput{
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				"SentTimestamp",
				"SenderId",
			},
		})
		requireSameSet(t, keys(a), []string{"SentTimestamp", "SenderId"}, "attribute names")
		// Epoch milliseconds (MessageSystemAttributeNames, SentTimestamp).
		ms := int64(atoi(t, a["SentTimestamp"], "SentTimestamp"))
		if d := time.UnixMilli(ms).Sub(sentAt); d < -2*time.Second || d > 2*time.Second {
			t.Fatalf("SentTimestamp %d is %v from the send", ms, d)
		}
	})

	t.Run("All", func(t *testing.T) {
		a := recvAttrs(t, c, u, &sqs.ReceiveMessageInput{
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{"All"},
		})
		for _, k := range []string{
			"SenderId", "SentTimestamp", "ApproximateReceiveCount",
			"ApproximateFirstReceiveTimestamp",
		} {
			if a[k] == "" {
				t.Fatalf("All lacks %s: %v", k, a)
			}
		}
		// The first receive was "none unless asked"; the count is cumulative.
		if atoi(t, a["ApproximateReceiveCount"], "ApproximateReceiveCount") < 3 {
			t.Fatalf(
				"ApproximateReceiveCount %s after three receives",
				a["ApproximateReceiveCount"],
			)
		}
	})

	t.Run("the deprecated AttributeNames", func(t *testing.T) {
		in := &sqs.ReceiveMessageInput{}
		//nolint:staticcheck // SA1019: the design keeps the deprecated parameter
		in.AttributeNames = []types.QueueAttributeName{"All"}
		a := recvAttrs(t, c, u, in)
		if a["SentTimestamp"] == "" || a["ApproximateReceiveCount"] == "" {
			t.Fatalf("AttributeNames All returned %v", a)
		}
	})

	t.Run("ApproximateFirstReceiveTimestamp stays the first", func(t *testing.T) {
		names := []types.MessageSystemAttributeName{"ApproximateFirstReceiveTimestamp"}
		first := recvAttrs(t, c, u, &sqs.ReceiveMessageInput{MessageSystemAttributeNames: names})
		second := recvAttrs(t, c, u, &sqs.ReceiveMessageInput{MessageSystemAttributeNames: names})
		requireEqual(t, second["ApproximateFirstReceiveTimestamp"],
			first["ApproximateFirstReceiveTimestamp"], "ApproximateFirstReceiveTimestamp")
	})
}

// recvAttrs receives the one message of u with in and returns its system
// attributes.
func recvAttrs(
	t *testing.T,
	c *sqs.Client,
	u string,
	in *sqs.ReceiveMessageInput,
) map[string]string {
	t.Helper()
	in.QueueUrl = aws.String(u)
	msgs := receiveInput(t, c, in)
	if len(msgs) != 1 {
		t.Fatalf("received %d messages, want 1", len(msgs))
	}
	return msgs[0].Attributes
}

func TestReceiveMessageAttributeSelection(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "msg-attrs", map[string]string{"VisibilityTimeout": "0"})
	attrs := map[string]types.MessageAttributeValue{
		"alpha":     stringAttr("String", "a"),
		"beta":      stringAttr("Number", "2"),
		"bar.one":   stringAttr("String", "b1"),
		"bar.two":   binaryAttr("Binary", []byte{1, 2}),
		"barnacles": stringAttr("String", "not under bar."),
	}
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("x"), MessageAttributes: attrs,
	})

	// "you can send a list of attribute names to receive, or you can return
	// all of the attributes by specifying All or .* in your request. You can
	// also use all message attributes starting with a prefix, for example
	// bar.*" (MessageAttributeNames).
	for _, tt := range []struct {
		names []string
		want  []string
	}{
		{names: nil, want: nil},
		{names: []string{"alpha"}, want: []string{"alpha"}},
		{names: []string{"alpha", "beta", "missing"}, want: []string{"alpha", "beta"}},
		{names: []string{"bar.*"}, want: []string{"bar.one", "bar.two"}},
		{names: []string{"All"}, want: keysOf(attrs)},
		{names: []string{".*"}, want: keysOf(attrs)},
	} {
		msgs := receiveInput(t, c, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(u), MessageAttributeNames: tt.names,
		})
		if len(msgs) != 1 {
			t.Fatalf("names %q: received %d messages, want 1", tt.names, len(msgs))
		}
		got := msgs[0].MessageAttributes
		requireSameSet(t, keysOf(got), tt.want, "attributes for "+strconvQuote(tt.names))
		if len(tt.want) == len(attrs) {
			requireAttrs(t, got, attrs)
			requireEqual(t, aws.ToString(msgs[0].MD5OfMessageAttributes), attrsMD5(attrs),
				"MD5OfMessageAttributes with every attribute")
		}
	}
}

func strconvQuote(ss []string) string {
	out := ""
	for _, s := range ss {
		out += strconv.Quote(s)
	}
	return out
}

func TestLongPollReturnsWhenAMessageArrives(t *testing.T) {
	// "If a message is available, the call returns sooner than
	// WaitTimeSeconds" (ReceiveMessage), and the design has long polling
	// wake on a send, not by sleeping.
	c, _ := newClient(t)
	u := createQueue(t, c, "polled", nil)

	type result struct {
		at   time.Time
		err  error
		msgs []types.Message
	}
	done := make(chan result, 1)
	go func() {
		out, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(u), WaitTimeSeconds: 20,
		})
		r := result{err: err, at: time.Now()}
		if out != nil {
			r.msgs = out.Messages
		}
		done <- r
	}()

	time.Sleep(500 * time.Millisecond)
	sentAt := time.Now()
	send(t, c, u, "wake up")

	select {
	case r := <-done:
		requireNoErr(t, r.err, "long poll")
		requireSlice(t, bodies(r.msgs), []string{"wake up"}, "long-polled bodies")
		if d := r.at.Sub(sentAt); d > 2*time.Second {
			t.Fatalf("the long poll returned %v after the send, want within 2s", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the long poll did not return within 10s of the send")
	}
}

func TestLongPollWaitsWhenEmpty(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "quiet", nil)

	took := within(t, 5*time.Second, "an empty 2-second long poll", func() {
		msgs := receiveInput(t, c, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(u), WaitTimeSeconds: 2,
		})
		requireEqual(t, len(msgs), 0, "messages")
	})
	if took < 1800*time.Millisecond {
		t.Fatalf("an empty 2-second long poll returned after %v", took)
	}

	// A short poll on an empty queue answers at once.
	within(t, time.Second, "an empty short poll", func() { requireEmpty(t, c, u, "short poll") })
}

func TestQueueReceiveWaitTimeApplies(t *testing.T) {
	// ReceiveMessageWaitTimeSeconds: "The length of time ... for which a
	// ReceiveMessage action waits for a message to arrive" when the request
	// sets none (the SDK leaves out a WaitTimeSeconds of 0).
	c, _ := newClient(t)
	u := createQueue(t, c, "waiting", map[string]string{"ReceiveMessageWaitTimeSeconds": "10"})

	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("late"),
		})
	}()
	within(t, 5*time.Second, "a receive on a queue that waits 10s", func() {
		out, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(u)})
		requireNoErr(t, err, "receive")
		requireSlice(t, bodies(out.Messages), []string{"late"}, "bodies")
	})
}
