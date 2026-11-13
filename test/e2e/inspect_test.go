//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The inspection API under /_queues/, as the design and its pinned shapes
// describe it. It is not AWS; SQS supplies only the attribute names and
// the error codes.

func TestInspectQueues(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	requireEqual(t, len(in.queues(t)), 0, "queues of an empty store")

	b := createQueue(t, c, "b-queue", nil)
	a := createQueue(t, c, "a-queue", map[string]string{"VisibilityTimeout": "45"})
	f := createFIFO(t, c, "c.fifo", nil)
	send(t, c, a, "visible")
	send(t, c, a, "in flight")
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(a), MessageBody: aws.String("delayed"), DelaySeconds: 600,
	})
	receiveOne(t, c, a)

	got := in.queues(t)
	names := make([]string, 0, len(got))
	for _, q := range got {
		names = append(names, q.Name)
	}
	requireSlice(t, names, []string{"a-queue", "b-queue", "c.fifo"}, "queues ordered by name")

	for i, u := range []string{a, b, f} {
		q := got[i]
		requireEqual(t, q.URL, u, "url of "+q.Name)
		requireMatch(t, arnPattern, q.ARN, "arn of "+q.Name)
		requireEqual(t, q.ARN, "arn:aws:sqs:us-east-1:000000000000:"+q.Name, "arn of "+q.Name)
		want := queueAttrs(t, c, u, "All")
		if len(q.Attributes) != len(want) {
			t.Fatalf(
				"attributes of %s: got %v, want GetQueueAttributes All %v",
				q.Name,
				q.Attributes,
				want,
			)
		}
		for k, v := range want {
			requireEqual(t, q.Attributes[k], v, q.Name+" attribute "+k)
		}
	}
	requireEqual(
		t,
		got[0].Counts,
		inspectedCounts{Visible: 1, InFlight: 1, Delayed: 1},
		"counts of a-queue",
	)
	requireEqual(t, got[1].Counts, inspectedCounts{}, "counts of b-queue")
}

func TestInspectMessagesLeavesThemAlone(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	u := createQueue(t, c, "watched", nil)
	attrs := map[string]types.MessageAttributeValue{
		"s": stringAttr("String", "text"),
		"b": binaryAttr("Binary", []byte{0, 1, 2, 255}),
	}
	visible := sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("visible"), MessageAttributes: attrs,
	})
	flying := send(t, c, u, "in flight")
	delayed := sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("delayed"), DelaySeconds: 600,
	})
	got := receiveInput(
		t,
		c,
		&sqs.ReceiveMessageInput{QueueUrl: aws.String(u), MaxNumberOfMessages: 2},
	)
	// Make "visible" visible again, so one of each state remains.
	for _, m := range got {
		if aws.ToString(m.MessageId) == aws.ToString(visible.MessageId) {
			changeVisibility(t, c, u, m, 0)
		}
	}
	before := time.Now()

	for range 2 {
		items := in.allMessages(t, "watched", nil)
		// "in queue order": the order of sending.
		requireSlice(t, idsOf(items), []string{
			aws.ToString(
				visible.MessageId,
			),
			aws.ToString(flying.MessageId),
			aws.ToString(delayed.MessageId),
		}, "listed ids")
		requireEqual(t, items[0].State, "visible", "state of the visible message")
		requireEqual(t, items[1].State, "inFlight", "state of the in-flight message")
		requireEqual(t, items[2].State, "delayed", "state of the delayed message")
		requireEqual(t, items[0].ReceiveCount, 1, "receiveCount of the visible message")
		requireEqual(t, items[1].ReceiveCount, 1, "receiveCount of the in-flight message")
		requireEqual(t, items[2].ReceiveCount, 0, "receiveCount of the delayed message")
		for _, m := range items[1:] {
			if !parseTime(t, m.VisibleAt, "visibleAt").After(before) {
				t.Fatalf("%s message visibleAt %s is not in the future", m.State, m.VisibleAt)
			}
		}
		requireEqual(t, items[0].Body, "visible", "body")
		requireInspectedAttrs(t, items[0].MessageAttributes, attrs)
		requireSystemAttrs(t, items)
	}

	requireCounts(t, c, u, counts{visible: 1, notVisible: 1, delayed: 1}, "after listing")
	m := receiveOne(t, c, u)
	requireEqual(
		t,
		aws.ToString(m.MessageId),
		aws.ToString(visible.MessageId),
		"the visible message",
	)
	requireEqual(t, receiveCount(t, m), 2, "receive count after listing twice")
}

func idsOf(items []inspectedMessage) []string {
	out := make([]string, 0, len(items))
	for _, m := range items {
		out = append(out, m.ID)
	}
	return out
}

func requireInspectedAttrs(
	t *testing.T,
	got map[string]inspectedAttr,
	want map[string]types.MessageAttributeValue,
) {
	t.Helper()
	requireSameSet(t, keysOf(got), keysOf(want), "messageAttributes names")
	for k, w := range want {
		g := got[k]
		requireEqual(t, g.DataType, aws.ToString(w.DataType), k+" dataType")
		if w.StringValue != nil {
			requireEqual(
				t,
				aws.ToString(g.StringValue),
				aws.ToString(w.StringValue),
				k+" stringValue",
			)
		} else if string(g.BinaryValue) != string(w.BinaryValue) {
			t.Fatalf("%s binaryValue: got %x, want %x", k, g.BinaryValue, w.BinaryValue)
		}
	}
}

// requireSystemAttrs checks the system attributes of the three messages of
// TestInspectMessagesLeavesThemAlone: ApproximateFirstReceiveTimestamp only
// once received.
func requireSystemAttrs(t *testing.T, items []inspectedMessage) {
	t.Helper()
	for _, m := range items {
		for _, k := range []string{"SenderId", "SentTimestamp", "ApproximateReceiveCount"} {
			if m.Attributes[k] == "" {
				t.Fatalf("message %s (%s) lacks attribute %s: %v", m.ID, m.State, k, m.Attributes)
			}
		}
		requireEqual(t, m.Attributes["ApproximateReceiveCount"], strconv.Itoa(m.ReceiveCount),
			"ApproximateReceiveCount against receiveCount")
		_, first := m.Attributes["ApproximateFirstReceiveTimestamp"]
		requireEqual(t, first, m.ReceiveCount > 0, m.State+" has ApproximateFirstReceiveTimestamp")
	}
}

func TestInspectFIFOMessages(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	u := createFIFO(t, c, "fields.fifo", nil)
	s1 := sendFIFO(t, c, u, "a1", "A", "da1")
	sendFIFO(t, c, u, "b1", "B", "db1")
	sendFIFO(t, c, u, "a2", "A", "da2")
	std := createQueue(t, c, "plain", nil)
	send(t, c, std, "plain")

	items := in.allMessages(t, "fields.fifo", url.Values{"group": {"A"}})
	requireSlice(t, bodiesOf(items), []string{"a1", "a2"}, "group A")
	requireEqual(t, items[0].Group, "A", "group")
	requireEqual(t, items[0].DeduplicationID, "da1", "deduplicationId")
	requireEqual(t, items[0].SequenceNumber, aws.ToString(s1.SequenceNumber), "sequenceNumber")
	requireEqual(t, items[0].Attributes["MessageGroupId"], "A", "MessageGroupId attribute")
	requireEqual(t, items[0].Attributes["SequenceNumber"], aws.ToString(s1.SequenceNumber),
		"SequenceNumber attribute")

	// Omitted when empty, on a standard queue.
	res := in.do(t, http.MethodGet, "/_queues/queues/plain/messages", nil)
	requireOK(t, res, "list plain")
	var raw struct {
		Items []map[string]json.RawMessage `json:"items"`
	}
	decode(t, res, &raw)
	requireEqual(t, len(raw.Items), 1, "plain items")
	for _, k := range []string{"group", "deduplicationId", "sequenceNumber"} {
		if v, ok := raw.Items[0][k]; ok {
			t.Fatalf("a standard message has %s=%s, want it omitted", k, v)
		}
	}
	if _, ok := raw.Items[0]["visibleAt"]; !ok {
		t.Fatal("visibleAt is omitted, want it always present")
	}
}

func bodiesOf(items []inspectedMessage) []string {
	out := make([]string, 0, len(items))
	for _, m := range items {
		out = append(out, m.Body)
	}
	return out
}

func TestInspectStateFilter(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	u := createQueue(t, c, "states", nil)
	send(t, c, u, "flying")
	receiveOne(t, c, u)
	send(t, c, u, "waiting")
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("sleeping"), DelaySeconds: 600,
	})

	for state, want := range map[string]string{"visible": "waiting", "inFlight": "flying", "delayed": "sleeping"} {
		items := in.allMessages(t, "states", url.Values{"state": {state}})
		requireSlice(t, bodiesOf(items), []string{want}, "state "+state)
	}
}

func TestInspectPagination(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	u := createQueue(t, c, "paged", nil)
	want := make([]string, 0, 25)
	for i := range 25 {
		want = append(want, aws.ToString(send(t, c, u, "m"+strconv.Itoa(i)).MessageId))
	}

	first := in.messages(t, "paged", nil)
	requireEqual(t, len(first.Items), 20, "default page size")
	if first.NextCursor == "" {
		t.Fatal("a first page of 20 of 25 has no nextCursor")
	}
	rest := in.messages(t, "paged", url.Values{"cursor": {first.NextCursor}})
	requireEqual(t, len(rest.Items), 5, "second page")
	requireEqual(t, rest.NextCursor, "", "nextCursor of the last page")
	requireSlice(t, append(idsOf(first.Items), idsOf(rest.Items)...), want, "ids over both pages")

	var sizes []string
	params := url.Values{"limit": {"7"}}
	for {
		p := in.messages(t, "paged", params)
		sizes = append(sizes, strconv.Itoa(len(p.Items)))
		if p.NextCursor == "" {
			break
		}
		params.Set("cursor", p.NextCursor)
	}
	requireSlice(t, sizes, []string{"7", "7", "7", "4"}, "page sizes with limit 7")
	requireEqual(
		t,
		len(in.messages(t, "paged", url.Values{"limit": {"100"}}).Items),
		25,
		"limit 100",
	)
}

func TestInspectErrors(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	createQueue(t, c, "real", nil)
	path := "/_queues/queues/real/messages"

	requireAPIErrorBody(t, in.do(t, http.MethodGet, "/_queues/queues/missing/messages", nil),
		http.StatusNotFound, "QueueDoesNotExist", "an unknown queue")

	for _, q := range []url.Values{
		{"state": {"sleeping"}},
		{"limit": {"0"}},
		{"limit": {"101"}},
		{"limit": {"ten"}},
		{"cursor": {"not-a-cursor"}},
	} {
		requireAPIErrorBody(t, in.do(t, http.MethodGet, path, q), http.StatusBadRequest,
			"InvalidParameterValue", "query "+q.Encode())
	}
}

func TestInspectRefusesBadSignatures(t *testing.T) {
	_, srv := newClient(t)
	requireAPIErrorBody(t, inspectorFor(srv).doUnsigned(t, http.MethodGet, "/_queues/queues"),
		http.StatusForbidden, "MissingAuthenticationToken", "unsigned")
	requireAPIErrorBody(t,
		inspectorWith(srv, aws.Credentials{AccessKeyID: "other", SecretAccessKey: "test"}).
			do(t, http.MethodGet, "/_queues/queues", nil),
		http.StatusForbidden, "InvalidClientTokenId", "unknown key")
	requireAPIErrorBody(t,
		inspectorWith(srv, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "wrong"}).
			do(t, http.MethodDelete, "/_queues/queues", nil),
		http.StatusForbidden, "SignatureDoesNotMatch", "wrong secret")
	requireEqual(
		t,
		len(inspectorFor(srv).queues(t)),
		0,
		"nothing deleted by the refused delete-all",
	)
}

func TestInspectPurgeAll(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	a := createQueue(t, c, "one", nil)
	b := createFIFO(t, c, "two.fifo", nil)
	send(t, c, a, "x")
	send(t, c, a, "y")
	receiveOne(t, c, a)
	sendFIFO(t, c, b, "z", "g", "d")

	in.purgeAll(t)

	requireCounts(t, c, a, counts{}, "one after purge-all")
	requireCounts(t, c, b, counts{}, "two.fifo after purge-all")
	requireEqual(t, len(in.queues(t)), 2, "queues kept")
	requireEqual(t, len(in.allMessages(t, "one", nil)), 0, "messages listed after purge-all")
	send(t, c, a, "after")
	requireEqual(t, aws.ToString(receiveOne(t, c, a).Body), "after", "the queue still works")
}

func TestInspectDeleteAll(t *testing.T) {
	c, srv := newClient(t)
	in := inspectorFor(srv)
	for _, n := range []string{"x", "y"} {
		send(t, c, createQueue(t, c, n, nil), "m")
	}

	in.deleteAll(t)

	requireEqual(t, len(in.queues(t)), 0, "queues after delete-all")
	out, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{})
	requireNoErr(t, err, "list queues")
	requireEqual(t, len(out.QueueUrls), 0, "ListQueues after delete-all")
	requireQueueMissing(t, c, "x")
	requireAPIErrorBody(t, in.do(t, http.MethodGet, "/_queues/queues/x/messages", nil),
		http.StatusNotFound, "QueueDoesNotExist", "a deleted queue")
}
