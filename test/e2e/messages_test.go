//go:build e2e

package e2e

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Sending, against SendMessage and SendMessageBatch (api_op_*.go), the SQS
// Developer Guide's message attribute rules and digest, and the errors of
// types/errors.go.

// allTypes is one attribute of each logical type and a custom label of
// each (Developer Guide, Message attribute data types).
func allTypes() map[string]types.MessageAttributeValue {
	return map[string]types.MessageAttributeValue{
		"text":      stringAttr("String", "héllo wörld"),
		"count":     stringAttr("Number", "42"),
		"ratio":     stringAttr("Number", "-3.5"),
		"blob":      binaryAttr("Binary", []byte{0x00, 0x01, 0xfe, 0xff}),
		"label":     stringAttr("String.label", "custom string"),
		"small":     stringAttr("Number.int", "7"),
		"picture":   binaryAttr("Binary.gif", []byte("GIF89a")),
		"dotted.nm": stringAttr("String", "dots inside a name"),
	}
}

func TestSendMessage(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "sent", nil)
	attrs := allTypes()

	out := sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl:          aws.String(u),
		MessageBody:       aws.String("payload ✓"),
		MessageAttributes: attrs,
	})
	requireEqual(t, aws.ToString(out.MD5OfMessageAttributes), attrsMD5(attrs),
		"MD5OfMessageAttributes")
	// "This parameter applies only to FIFO queues" (SequenceNumber).
	requireEqual(t, aws.ToString(out.SequenceNumber), "", "SequenceNumber on a standard queue")

	got := receiveOne(t, c, u)
	requireEqual(t, aws.ToString(got.MessageId), aws.ToString(out.MessageId), "MessageId")
	requireEqual(t, aws.ToString(got.Body), "payload ✓", "Body")
	requireEqual(t, aws.ToString(got.MD5OfMessageAttributes), attrsMD5(attrs),
		"MD5OfMessageAttributes on receive")
	requireAttrs(t, got.MessageAttributes, attrs)
}

func TestMessageIDsAreDistinct(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "ids", nil)
	seen := map[string]bool{}
	for i := range 5 {
		id := aws.ToString(send(t, c, u, "same body").MessageId)
		if seen[id] {
			t.Fatalf("send %d returned the MessageId %s again", i, id)
		}
		seen[id] = true
	}
}

// requireAttrs fails unless got holds exactly want's attributes, with the
// same data types and values.
func requireAttrs(t *testing.T, got, want map[string]types.MessageAttributeValue) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("message attributes: got %d (%v), want %d", len(got), keysOf(got), len(want))
	}
	for name, w := range want {
		g, ok := got[name]
		if !ok {
			t.Fatalf("message attribute %s missing; got %v", name, keysOf(got))
		}
		requireEqual(t, aws.ToString(g.DataType), aws.ToString(w.DataType), name+" DataType")
		requireEqual(
			t,
			aws.ToString(g.StringValue),
			aws.ToString(w.StringValue),
			name+" StringValue",
		)
		if !bytes.Equal(g.BinaryValue, w.BinaryValue) {
			t.Fatalf("%s BinaryValue: got %x, want %x", name, g.BinaryValue, w.BinaryValue)
		}
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestMessageSystemAttributes(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "traced", nil)
	// An X-Ray trace header in the documented format.
	const trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
	sys := map[string]types.MessageSystemAttributeValue{
		"AWSTraceHeader": {DataType: aws.String("String"), StringValue: aws.String(trace)},
	}

	out := sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl:                aws.String(u),
		MessageBody:             aws.String("traced"),
		MessageSystemAttributes: sys,
	})
	requireEqual(t, aws.ToString(out.MD5OfMessageSystemAttributes), systemAttrsMD5(sys),
		"MD5OfMessageSystemAttributes")
	requireEqual(t, aws.ToString(out.MD5OfMessageAttributes), "",
		"MD5OfMessageAttributes with no message attributes")

	msgs := receiveInput(t, c, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(u),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{"AWSTraceHeader"},
	})
	if len(msgs) != 1 {
		t.Fatalf("received %d messages, want 1", len(msgs))
	}
	requireEqual(t, msgs[0].Attributes["AWSTraceHeader"], trace, "AWSTraceHeader")
}

func TestMessageAttributeNameRules(t *testing.T) {
	// Developer Guide, Message attribute components: a name of letters,
	// digits, '_', '-' and '.', up to 256, not starting with AWS. or Amazon.
	// in any case, not starting or ending with '.', no '..'. The refusal's
	// code is not documented; a 400 is asserted.
	c, _ := newClient(t)
	u := createQueue(t, c, "named", nil)

	for _, name := range []string{
		"AWS.reserved", "amazon.reserved", ".lead", "trail.", "two..dots",
		"sp ace", strings.Repeat("n", 257),
	} {
		t.Run(name[:min(len(name), 16)], func(t *testing.T) {
			err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
				QueueUrl:    aws.String(u),
				MessageBody: aws.String("x"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					name: stringAttr("String", "v"),
				},
			}))
			requireRefused(t, err)
		})
	}
	requireCounts(t, c, u, counts{}, "nothing stored by refused sends")

	// The longest name is accepted.
	long := strings.Repeat("n", 256)
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl:          aws.String(u),
		MessageBody:       aws.String("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{long: stringAttr("String", "v")},
	})
}

func TestMessageAttributeLimits(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "limited", nil)

	ten := map[string]types.MessageAttributeValue{}
	for i := range 10 {
		ten["a"+strconv.Itoa(i)] = stringAttr("String", "v")
	}
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("ten"), MessageAttributes: ten,
	})

	// "Each message can have up to 10 attributes" (Developer Guide). The
	// code is not documented.
	ten["a10"] = stringAttr("String", "v")
	requireRefused(t, errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("eleven"), MessageAttributes: ten,
	})))

	// A Number value that is not a number; "Amazon SQS validates numerical
	// values" (Message attribute data types). The code is not documented.
	requireRefused(t, errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(u),
		MessageBody: aws.String("nan"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"n": stringAttr("Number", "twelve"),
		},
	})))
	requireCounts(t, c, u, counts{visible: 1}, "only the accepted send stored")
}

func TestSendMessageBodyRules(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "bodies", nil)

	t.Run("every allowed character range round-trips", func(t *testing.T) {
		body := "tab\tnl\ncr\r é퟿�\U00010000\U0010ffff"
		send(t, c, u, body)
		requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), body, "body")
	})

	t.Run("a character outside the set is InvalidMessageContents", func(t *testing.T) {
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("bell\u0007"),
		}))
		requireModeled[*types.InvalidMessageContents](t, err, "InvalidMessageContents")
	})

	t.Run("1 MiB is the default limit", func(t *testing.T) {
		// "The maximum size is 1 MiB or 1,048,576 bytes" (SendMessage).
		send(t, c, u, strings.Repeat("a", 1048576))
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String(strings.Repeat("a", 1048577)),
		}))
		requireInvalidParameter(t, err) // Assumption: Common Errors' out-of-range code.
	})

	t.Run("the queue's MaximumMessageSize is the limit", func(t *testing.T) {
		small := createQueue(t, c, "small", map[string]string{"MaximumMessageSize": "1024"})
		send(t, c, small, strings.Repeat("b", 1024))
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(small), MessageBody: aws.String(strings.Repeat("b", 1025)),
		}))
		requireInvalidParameter(t, err) // Assumption: Common Errors' out-of-range code.
	})

	t.Run("an empty body is refused", func(t *testing.T) {
		// "The minimum size is one character" (SendMessage). Code undocumented.
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String(""),
		}))
		requireRefused(t, err)
	})

	t.Run("DelaySeconds above 900 is InvalidParameterValue", func(t *testing.T) {
		err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u), MessageBody: aws.String("late"), DelaySeconds: 901,
		}))
		requireInvalidParameter(t, err)
	})

	t.Run("a missing queue is QueueDoesNotExist", func(t *testing.T) {
		requireNonExistent(t, errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(u + "-missing"), MessageBody: aws.String("x"),
		})))
	})
}

func TestSendMessageBatch(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "batched", nil)
	attrs := map[string]types.MessageAttributeValue{"k": stringAttr("String", "v")}

	entries := make([]types.SendMessageBatchRequestEntry, 0, 10)
	for i := range 10 {
		e := types.SendMessageBatchRequestEntry{
			Id:          aws.String("e-" + strconv.Itoa(i)),
			MessageBody: aws.String("batch " + strconv.Itoa(i)),
		}
		if i == 0 {
			e.MessageAttributes = attrs
		}
		entries = append(entries, e)
	}
	out, err := c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(u), Entries: entries,
	})
	requireNoErr(t, err, "send message batch")
	requireEqual(t, len(out.Failed), 0, "failed entries")
	requireEqual(t, len(out.Successful), 10, "successful entries")
	for _, s := range out.Successful {
		i := atoi(t, strings.TrimPrefix(aws.ToString(s.Id), "e-"), "entry id")
		requireEqual(t, aws.ToString(s.MD5OfMessageBody), md5Hex("batch "+strconv.Itoa(i)),
			"MD5OfMessageBody of "+aws.ToString(s.Id))
		if aws.ToString(s.MessageId) == "" {
			t.Fatalf("entry %s has no MessageId", aws.ToString(s.Id))
		}
		if i == 0 {
			requireEqual(t, aws.ToString(s.MD5OfMessageAttributes), attrsMD5(attrs),
				"MD5OfMessageAttributes of e-0")
		}
	}
	requireCounts(t, c, u, counts{visible: 10}, "after the batch")
}

func TestSendMessageBatchPartialFailure(t *testing.T) {
	// "The result of sending each message is reported individually"
	// (SendMessageBatch). Which code a failed entry carries is not
	// documented; the entry, its sender fault and a code are asserted.
	c, _ := newClient(t)
	u := createQueue(t, c, "partial", nil)

	out, err := c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(u),
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("good-1"), MessageBody: aws.String("one")},
			{Id: aws.String("bad-chars"), MessageBody: aws.String("bell\u0007")},
			{Id: aws.String("good-2"), MessageBody: aws.String("two")},
			{Id: aws.String("bad-delay"), MessageBody: aws.String("late"), DelaySeconds: 901},
		},
	})
	requireNoErr(t, err, "send message batch")

	ok := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		ok = append(ok, aws.ToString(s.Id))
	}
	requireSameSet(t, ok, []string{"good-1", "good-2"}, "successful entries")
	requireBatchFailed(t, out.Failed, "bad-chars", "bad-delay")

	requireSameSet(t, bodies(receive(t, c, u, 10)), []string{"one", "two"}, "stored bodies")
}

// requireBatchFailed fails unless failed holds exactly the entries ids, each
// a sender fault with a code.
func requireBatchFailed(t *testing.T, failed []types.BatchResultErrorEntry, ids ...string) {
	t.Helper()
	got := make([]string, 0, len(failed))
	for _, f := range failed {
		got = append(got, aws.ToString(f.Id))
		if aws.ToString(f.Code) == "" {
			t.Fatalf("failed entry %s has no code", aws.ToString(f.Id))
		}
		if !f.SenderFault {
			t.Fatalf("failed entry %s (%s) is not a sender fault", aws.ToString(f.Id),
				aws.ToString(f.Code))
		}
	}
	requireSameSet(t, got, ids, "failed entries")
}

func TestSendMessageBatchLimits(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "limits", nil)
	entry := func(id, body string) types.SendMessageBatchRequestEntry {
		return types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String(body)}
	}
	batch := func(entries ...types.SendMessageBatchRequestEntry) error {
		return errOf(c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(u), Entries: entries,
		}, skipValidation))
	}

	t.Run("eleven entries is TooManyEntriesInBatchRequest", func(t *testing.T) {
		es := make([]types.SendMessageBatchRequestEntry, 0, 11)
		for i := range 11 {
			es = append(es, entry("e"+strconv.Itoa(i), "x"))
		}
		requireModeled[*types.TooManyEntriesInBatchRequest](t, batch(es...),
			"AWS.SimpleQueueService.TooManyEntriesInBatchRequest")
	})
	t.Run("no entries is EmptyBatchRequest", func(t *testing.T) {
		requireModeled[*types.EmptyBatchRequest](t, batch(),
			"AWS.SimpleQueueService.EmptyBatchRequest")
	})
	t.Run("a repeated Id is BatchEntryIdsNotDistinct", func(t *testing.T) {
		requireModeled[*types.BatchEntryIdsNotDistinct](
			t,
			batch(entry("same", "a"), entry("same", "b")),
			"AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
		)
	})
	t.Run("a malformed Id is InvalidBatchEntryId", func(t *testing.T) {
		// Ids are up to 80 letters, digits, hyphens and underscores
		// (SendMessageBatchRequestEntry, Id).
		requireModeled[*types.InvalidBatchEntryId](t, batch(entry("bad id!", "a")),
			"AWS.SimpleQueueService.InvalidBatchEntryId")
		requireModeled[*types.InvalidBatchEntryId](t, batch(entry(strings.Repeat("i", 81), "a")),
			"AWS.SimpleQueueService.InvalidBatchEntryId")
	})
	t.Run("more than 1 MiB in total is BatchRequestTooLong", func(t *testing.T) {
		// "the maximum total payload size ... 1 MiB" (SendMessageBatch).
		half := strings.Repeat("h", 600*1024)
		requireModeled[*types.BatchRequestTooLong](t, batch(entry("a", half), entry("b", half)),
			"AWS.SimpleQueueService.BatchRequestTooLong")
	})
	t.Run("a missing queue is QueueDoesNotExist", func(t *testing.T) {
		requireNonExistent(t, errOf(c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(
				u + "-missing",
			),
			Entries: []types.SendMessageBatchRequestEntry{entry("a", "x")},
		})))
	})

	requireCounts(t, c, u, counts{}, "nothing stored by refused batches")
}
