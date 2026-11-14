//go:build e2e

package e2e

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

// Message move tasks, the dead-letter queue redrive: StartMessageMoveTask,
// ListMessageMoveTasks and CancelMessageMoveTask against their API
// reference pages and the SDK model, and the Developer Guide's "Learn how
// to configure a dead-letter queue redrive in Amazon SQS" (DLQ redrive
// guide below).
//
// The API reference lists InvalidAddress, InvalidSecurity,
// RequestThrottled, ResourceNotFoundException and UnsupportedOperation for
// all three actions without saying which condition raises which, so most
// refusals here assert only a client error. ResourceNotFoundException is
// "One or more specified resources don't exist", which is asserted for a
// queue that does not exist. Its status is 400 on the API reference pages
// and 404 in the SDK schema, so no status is asserted for it.

// moveBound bounds the wait for a task to end.
const moveBound = 15 * time.Second

// The five task statuses ListMessageMoveTasksResultEntry's Status lists,
// spelled as AWS spells them.
const (
	moveRunning    = "RUNNING"
	moveCompleted  = "COMPLETED"
	moveCancelling = "CANCELLING" //nolint:misspell // the AWS status value
	moveCancelled  = "CANCELLED"  //nolint:misspell // the AWS status value
	moveFailed     = "FAILED"
)

// moveQueues is a dead-letter queue and the source queues that use it.
type moveQueues struct {
	dlq     string
	dlqARN  string
	sources []string
}

func arnOf(t *testing.T, c *sqs.Client, u string) string {
	t.Helper()
	return queueAttrs(t, c, u, "QueueArn")["QueueArn"]
}

// withDLQ creates the dead-letter queue dlqName and, for each of sources,
// a source queue whose RedrivePolicy moves a message there after one
// receive.
func withDLQ(t *testing.T, c *sqs.Client, dlqName string, sources ...string) moveQueues {
	t.Helper()
	q := moveQueues{dlq: createQueue(t, c, dlqName, nil)}
	q.dlqARN = arnOf(t, c, q.dlq)
	for _, name := range sources {
		q.sources = append(q.sources, createQueue(t, c, name, map[string]string{
			"RedrivePolicy": redrivePolicy(q.dlqARN, 1),
		}))
	}
	return q
}

// deadLetter sends bodies to src with attrs and drives every one of them
// into src's dead-letter queue the way SQS does: "When the ReceiveCount
// for a message exceeds the maxReceiveCount for a queue, Amazon SQS moves
// the message to the dead-letter-queue" (CreateQueue, RedrivePolicy). It
// returns the IDs the messages were sent with.
func deadLetter(
	t *testing.T,
	c *sqs.Client,
	src string,
	attrs map[string]types.MessageAttributeValue,
	bodies ...string,
) []string {
	t.Helper()
	ids := make([]string, 0, len(bodies))
	for _, b := range bodies {
		out := sendInput(t, c, &sqs.SendMessageInput{
			QueueUrl: aws.String(src), MessageBody: aws.String(b), MessageAttributes: attrs,
		})
		ids = append(ids, aws.ToString(out.MessageId))
	}
	var got []types.Message
	for range 10 {
		if len(got) == len(bodies) {
			break
		}
		got = append(got, receive(t, c, src, 10)...)
	}
	requireEqual(t, len(got), len(bodies), "messages received once before the DLQ")
	for _, m := range got {
		changeVisibility(t, c, src, m, 0)
	}
	// The next receive exceeds maxReceiveCount 1 for every one of them.
	requireEmpty(t, c, src, "the source queue once its messages went to the DLQ")
	return ids
}

// fillDLQ sends n messages straight to the dead-letter queue dlq, for
// tasks that name their DestinationArn.
func fillDLQ(t *testing.T, c *sqs.Client, dlq string, n int) {
	t.Helper()
	for i := range n {
		send(t, c, dlq, "m"+strconv.Itoa(i))
	}
	requireCounts(t, c, dlq, counts{visible: n}, "the filled DLQ")
}

// startMove starts a task and returns its TaskHandle.
func startMove(t *testing.T, c *sqs.Client, in *sqs.StartMessageMoveTaskInput) string {
	t.Helper()
	out, err := c.StartMessageMoveTask(t.Context(), in)
	requireNoErr(t, err, "start message move task")
	if aws.ToString(out.TaskHandle) == "" {
		t.Fatal("StartMessageMoveTask returned no TaskHandle")
	}
	return aws.ToString(out.TaskHandle)
}

// moveTo is a start request from sourceARN to destARN at rate messages per
// second; "" and 0 leave them out.
func moveTo(sourceARN, destARN string, rate int32) *sqs.StartMessageMoveTaskInput {
	in := &sqs.StartMessageMoveTaskInput{SourceArn: aws.String(sourceARN)}
	if destARN != "" {
		in.DestinationArn = aws.String(destARN)
	}
	if rate != 0 {
		in.MaxNumberOfMessagesPerSecond = aws.Int32(rate)
	}
	return in
}

func listMoves(
	t *testing.T,
	c *sqs.Client,
	sourceARN string,
	maxResults *int32,
) []types.ListMessageMoveTasksResultEntry {
	t.Helper()
	out, err := c.ListMessageMoveTasks(t.Context(), &sqs.ListMessageMoveTasksInput{
		SourceArn: aws.String(sourceARN), MaxResults: maxResults,
	})
	requireNoErr(t, err, "list message move tasks")
	return out.Results
}

// latestMove is the most recent task: "The default is 1, which provides
// the most recent message movement task" (ListMessageMoveTasks,
// MaxResults).
func latestMove(
	t *testing.T,
	c *sqs.Client,
	sourceARN string,
) types.ListMessageMoveTasksResultEntry {
	t.Helper()
	got := listMoves(t, c, sourceARN, nil)
	if len(got) != 1 {
		t.Fatalf("ListMessageMoveTasks with the default MaxResults: %d results, want 1", len(got))
	}
	return got[0]
}

// waitMove polls the most recent task of sourceARN until it ends, and
// returns it.
func waitMove(t *testing.T, c *sqs.Client, sourceARN string) types.ListMessageMoveTasksResultEntry {
	t.Helper()
	deadline := time.Now().Add(moveBound)
	for {
		e := latestMove(t, c, sourceARN)
		switch s := aws.ToString(e.Status); s {
		case moveCompleted, moveCancelled, moveFailed:
			return e
		case moveRunning, moveCancelling:
		default:
			t.Fatalf("task status %q is not one the API reference lists", s)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the task on %s did not end within %v: %+v", sourceARN, moveBound, e)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// requireEnded fails unless e ended in status with moved messages moved,
// from sourceARN to destARN at rate ("" and 0 for none given). "If a
// DestinationArn has not been specified ... this field value will be
// NULL", and the same for MaxNumberOfMessagesPerSecond; TaskHandle "is
// only populated for tasks that are in RUNNING status"; FailureReason is
// "only included if the task status is FAILED"
// (ListMessageMoveTasksResultEntry).
func requireEnded(
	t *testing.T,
	e types.ListMessageMoveTasksResultEntry,
	status string,
	moved int64,
	sourceARN, destARN string,
	rate int32,
) {
	t.Helper()
	requireEqual(t, aws.ToString(e.Status), status, "Status")
	// Assumption: the approximate count is exact on a single local server.
	requireEqual(t, e.ApproximateNumberOfMessagesMoved, moved, "ApproximateNumberOfMessagesMoved")
	requireEqual(t, aws.ToString(e.SourceArn), sourceARN, "SourceArn")
	requireOptional(t, e.DestinationArn, destARN, "", "DestinationArn")
	requireOptional(t, e.MaxNumberOfMessagesPerSecond, rate, 0, "MaxNumberOfMessagesPerSecond")
	requireEqual(t, aws.ToString(e.TaskHandle), "", "TaskHandle of an ended task")
	if status != moveFailed {
		requireEqual(t, aws.ToString(e.FailureReason), "", "FailureReason")
	}
}

// requireOptional fails unless got is nil when want is none, and points to
// want otherwise.
func requireOptional[T comparable](t *testing.T, got *T, want, none T, what string) {
	t.Helper()
	switch {
	case want == none && got != nil:
		t.Fatalf("%s: got %v, want none", what, *got)
	case want != none && got == nil:
		t.Fatalf("%s: got none, want %v", what, want)
	case want != none && *got != want:
		t.Fatalf("%s: got %v, want %v", what, *got, want)
	}
}

// requireStartedBetween fails unless StartedTimestamp, "The timestamp of
// starting the message movement task", lies between before and after.
// Assumption: milliseconds since the epoch, as the API reference's example
// (1684429053010); a second of slack each side allows a coarser clock.
func requireStartedBetween(t *testing.T, ts int64, before, after time.Time) {
	t.Helper()
	lo, hi := before.Add(-time.Second).UnixMilli(), after.Add(time.Second).UnixMilli()
	if ts < lo || ts > hi {
		t.Fatalf("StartedTimestamp %d is not between %d and %d (epoch milliseconds)", ts, lo, hi)
	}
}

// requireClientError fails unless err is an API error with a 4xx status,
// for refusals whose code no AWS source pins. InvalidAction, an operation
// the service does not know (Common Errors), is not such a refusal.
func requireClientError(t *testing.T, err error, what string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("%s: want an API error, got %v", what, err)
	}
	if apiErr.ErrorCode() == "InvalidAction" {
		t.Fatalf("%s: the operation is not served: %v", what, err)
	}
	var re *awshttp.ResponseError
	if !errors.As(err, &re) || re.HTTPStatusCode() < 400 || re.HTTPStatusCode() > 499 {
		t.Fatalf("%s: want a 4xx client error, got %v", what, err)
	}
}

// requireNotFound fails unless err is ResourceNotFoundException.
func requireNotFound(t *testing.T, err error) {
	t.Helper()
	requireAs[*types.ResourceNotFoundException](t, err)
	requireCode(t, err, "ResourceNotFoundException")
}

// requireRedriven fails unless u holds exactly bodies, each with attrs, as
// new messages: "All redriven messages are considered new messages with a
// new messageID" (DLQ redrive guide). oldIDs are the IDs they were first
// sent with, want the bodies.
func requireRedriven(
	t *testing.T,
	c *sqs.Client,
	u string,
	attrs map[string]types.MessageAttributeValue,
	oldIDs []string,
	want ...string,
) {
	t.Helper()
	msgs := receive(t, c, u, 10)
	requireSameSet(t, bodies(msgs), want, "redriven bodies in "+u)
	for _, m := range msgs {
		requireAttrs(t, m.MessageAttributes, attrs)
		requireEqual(t, aws.ToString(m.MD5OfMessageAttributes), attrsMD5(attrs),
			"MD5OfMessageAttributes of a redriven message")
		if slices.Contains(oldIDs, aws.ToString(m.MessageId)) {
			t.Fatalf("redriven message kept its MessageId %s", aws.ToString(m.MessageId))
		}
		// Assumption: a new message starts its receive count afresh.
		requireEqual(t, receiveCount(t, m), 1, "receive count of a redriven message")
	}
}

func TestMoveTaskBackToTheOriginalSources(t *testing.T) {
	// "If this field is left blank, the messages will be redriven back to
	// their respective original source queues" (DestinationArn).
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work-a", "work-b")
	attrs := allTypes()
	idsA := deadLetter(t, c, q.sources[0], attrs, "for-a-1", "for-a-2")
	idsB := deadLetter(t, c, q.sources[1], attrs, "for-b")
	requireCounts(t, c, q.dlq, counts{visible: 3}, "the DLQ before the redrive")

	before := time.Now()
	startMove(t, c, moveTo(q.dlqARN, "", 0))
	after := time.Now()
	done := waitMove(t, c, q.dlqARN)
	requireEnded(t, done, moveCompleted, 3, q.dlqARN, "", 0)
	requireStartedBetween(t, done.StartedTimestamp, before, after)

	requireCounts(t, c, q.dlq, counts{}, "the DLQ after the redrive")
	requireRedriven(t, c, q.sources[0], attrs, idsA, "for-a-1", "for-a-2")
	requireRedriven(t, c, q.sources[1], attrs, idsB, "for-b")
}

func TestMoveTaskToADestination(t *testing.T) {
	// "You can use this field to specify the destination queue where you
	// would like to redrive messages" (DestinationArn).
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	dest := createQueue(t, c, "elsewhere", nil)
	destARN := arnOf(t, c, dest)
	attrs := map[string]types.MessageAttributeValue{"kind": stringAttr("String", "poison")}
	ids := deadLetter(t, c, q.sources[0], attrs, "one", "two")

	startMove(t, c, moveTo(q.dlqARN, destARN, 50))
	requireEnded(t, waitMove(t, c, q.dlqARN), moveCompleted, 2, q.dlqARN, destARN, 50)

	requireCounts(t, c, q.dlq, counts{}, "the DLQ after the redrive")
	requireRedriven(t, c, dest, attrs, ids, "one", "two")
	requireEmpty(t, c, q.sources[0], "the original source of a redrive elsewhere")
}

func TestMoveTaskRate(t *testing.T) {
	// "The number of messages to be moved per second (the message movement
	// rate)" (MaxNumberOfMessagesPerSecond). Assumption: ten messages at
	// five a second take at least a second, whatever burst the first
	// second allows.
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	dest := createQueue(t, c, "dest", nil)
	fillDLQ(t, c, q.dlq, 10)

	start := time.Now()
	startMove(t, c, moveTo(q.dlqARN, arnOf(t, c, dest), 5))
	requireEnded(t, waitMove(t, c, q.dlqARN), moveCompleted, 10, q.dlqARN, arnOf(t, c, dest), 5)
	if took := time.Since(start); took < 800*time.Millisecond {
		t.Fatalf("ten messages at 5 per second moved in %v, want at least about 1s", took)
	}
	requireCounts(t, c, dest, counts{visible: 10}, "the destination")
}

func TestMoveTaskWakesALongPoll(t *testing.T) {
	// A message a redrive puts on the source queue is "available" to a
	// waiting ReceiveMessage, which "returns sooner than WaitTimeSeconds"
	// (ReceiveMessage).
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	deadLetter(t, c, q.sources[0], nil, "come back")

	type result struct {
		err  error
		msgs []types.Message
	}
	done := make(chan result, 1)
	go func() {
		out, err := c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(q.sources[0]), WaitTimeSeconds: 10,
		})
		r := result{err: err}
		if out != nil {
			r.msgs = out.Messages
		}
		done <- r
	}()

	time.Sleep(500 * time.Millisecond)
	startMove(t, c, moveTo(q.dlqARN, "", 0))

	select {
	case r := <-done:
		requireNoErr(t, r.err, "long poll")
		requireSlice(t, bodies(r.msgs), []string{"come back"}, "long-polled bodies")
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll did not return within 5s of the redrive")
	}
}

func TestCancelMessageMoveTask(t *testing.T) {
	// CancelMessageMoveTask does not revert the messages already moved and
	// stops only those not moved yet (CancelMessageMoveTask).
	const n = 10
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	dest := createQueue(t, c, "dest", nil)
	destARN := arnOf(t, c, dest)
	fillDLQ(t, c, q.dlq, n)

	// At one message a second the task is still running when it is
	// canceled.
	handle := startMove(t, c, moveTo(q.dlqARN, destARN, 1))
	out, err := c.CancelMessageMoveTask(t.Context(), &sqs.CancelMessageMoveTaskInput{
		TaskHandle: aws.String(handle),
	})
	requireNoErr(t, err, "cancel the running task")

	done := waitMove(t, c, q.dlqARN)
	moved := done.ApproximateNumberOfMessagesMoved
	requireEnded(t, done, moveCancelled, moved, q.dlqARN, destARN, 1)
	if moved >= n {
		t.Fatalf("the canceled task moved %d of %d messages, want fewer", moved, n)
	}
	if out.ApproximateNumberOfMessagesMoved > moved {
		t.Fatalf("cancel reported %d moved, more than the %d the ended task reports",
			out.ApproximateNumberOfMessagesMoved, moved)
	}
	requireCounts(t, c, dest, counts{visible: int(moved)}, "the destination after the cancel")
	requireCounts(t, c, q.dlq, counts{visible: n - int(moved)}, "the DLQ after the cancel")

	// Only a RUNNING task can be canceled (CancelMessageMoveTask).
	requireClientError(t, errOf(c.CancelMessageMoveTask(t.Context(),
		&sqs.CancelMessageMoveTaskInput{TaskHandle: aws.String(handle)})),
		"cancel a canceled task")

	// With no task active, a new one moves the rest.
	startMove(t, c, moveTo(q.dlqARN, destARN, 0))
	requireEnded(t, waitMove(t, c, q.dlqARN), moveCompleted, n-moved, q.dlqARN, destARN, 0)
	requireCounts(t, c, dest, counts{visible: n}, "the destination after the second task")
}

func TestMoveTaskOneActivePerSource(t *testing.T) {
	// "Only one active message movement task is supported per queue at any
	// given time" (StartMessageMoveTask).
	const n = 10
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	dest := createQueue(t, c, "dest", nil)
	destARN := arnOf(t, c, dest)
	fillDLQ(t, c, q.dlq, n)

	startMove(t, c, moveTo(q.dlqARN, destARN, 1))
	requireClientError(t, errOf(c.StartMessageMoveTask(t.Context(), moveTo(q.dlqARN, destARN, 0))),
		"a second task while one runs")

	running := latestMove(t, c, q.dlqARN)
	requireEqual(t, aws.ToString(running.Status), moveRunning, "Status of the first task")
	requireEqual(t, aws.ToString(running.SourceArn), q.dlqARN, "SourceArn")
	requireOptional(t, running.DestinationArn, destARN, "", "DestinationArn")
	requireOptional(t, running.MaxNumberOfMessagesPerSecond, 1, 0, "MaxNumberOfMessagesPerSecond")
	requireEqual(t, aws.ToString(running.FailureReason), "", "FailureReason")
	if toMove := running.ApproximateNumberOfMessagesToMove; toMove != nil &&
		(*toMove < 0 || *toMove > n) {
		t.Fatalf("ApproximateNumberOfMessagesToMove %d is outside 0..%d", *toMove, n)
	}

	// The handle a RUNNING entry carries cancels it.
	if aws.ToString(running.TaskHandle) == "" {
		t.Fatal("a RUNNING task has no TaskHandle")
	}
	_, err := c.CancelMessageMoveTask(t.Context(), &sqs.CancelMessageMoveTaskInput{
		TaskHandle: running.TaskHandle,
	})
	requireNoErr(t, err, "cancel with the listed handle")
	requireEqual(t, aws.ToString(waitMove(t, c, q.dlqARN).Status), moveCancelled, "Status")
}

func TestListMessageMoveTasks(t *testing.T) {
	// "Gets the most recent message movement tasks (up to 10)"; MaxResults:
	// "The default is 1, which provides the most recent message movement
	// task. The upper limit is 10."
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	destARN := arnOf(t, c, createQueue(t, c, "dest", nil))

	// Assumption: a queue with no tasks lists none.
	requireEqual(t, len(listMoves(t, c, q.dlqARN, aws.Int32(10))), 0, "tasks before any")

	// Eleven tasks one after another, told apart by their rates 101..111.
	for i := range int32(11) {
		fillDLQ(t, c, q.dlq, 1)
		startMove(t, c, moveTo(q.dlqARN, destARN, 101+i))
		requireEnded(t, waitMove(t, c, q.dlqARN), moveCompleted, 1, q.dlqARN, destARN, 101+i)
	}

	requireEqual(t, rateOf(t, latestMove(t, c, q.dlqARN)), 111, "the default lists the latest")
	requireRates(t, listMoves(t, c, q.dlqARN, aws.Int32(1)), 111, 111)
	requireRates(t, listMoves(t, c, q.dlqARN, aws.Int32(3)), 109, 111)
	requireRates(t, listMoves(t, c, q.dlqARN, aws.Int32(10)), 102, 111)

	// Whether a MaxResults above 10 is refused or capped is not documented;
	// either way no more than 10 come back.
	out, err := c.ListMessageMoveTasks(t.Context(), &sqs.ListMessageMoveTasksInput{
		SourceArn: aws.String(q.dlqARN), MaxResults: aws.Int32(11),
	})
	if err != nil {
		requireClientError(t, err, "MaxResults 11")
	} else if len(out.Results) > 10 {
		t.Fatalf("MaxResults 11 listed %d tasks, more than the upper limit of 10", len(out.Results))
	}
}

func rateOf(t *testing.T, e types.ListMessageMoveTasksResultEntry) int32 {
	t.Helper()
	if e.MaxNumberOfMessagesPerSecond == nil {
		t.Fatalf("task without MaxNumberOfMessagesPerSecond: %+v", e)
	}
	return *e.MaxNumberOfMessagesPerSecond
}

// requireRates fails unless got are the tasks with rates lo..hi. The order
// is not asserted: the API reference's example lists a COMPLETED task
// before a RUNNING one that started later.
func requireRates(t *testing.T, got []types.ListMessageMoveTasksResultEntry, lo, hi int32) {
	t.Helper()
	rates := make([]string, 0, len(got))
	for _, e := range got {
		rates = append(rates, strconv.Itoa(int(rateOf(t, e))))
	}
	want := make([]string, 0, hi-lo+1)
	for r := lo; r <= hi; r++ {
		want = append(want, strconv.Itoa(int(r)))
	}
	requireSameSet(t, rates, want, "the rates of the listed tasks")
}

func TestMoveTaskErrors(t *testing.T) {
	c, _ := newClient(t)
	q := withDLQ(t, c, "dead", "work")
	fillDLQ(t, c, q.dlq, 1)
	refused := func(t *testing.T, in *sqs.StartMessageMoveTaskInput, what string) {
		t.Helper()
		requireClientError(t, errOf(c.StartMessageMoveTask(t.Context(), in)), what)
		requireCounts(t, c, q.dlq, counts{visible: 1}, "the DLQ after a refused start")
	}

	t.Run("a queue that is no dead-letter queue", func(t *testing.T) {
		// "only ARNs of dead-letter queues (DLQs) whose sources are other
		// Amazon SQS queues are accepted" (SourceArn).
		plain := createQueue(t, c, "plain", nil)
		send(t, c, plain, "stays")
		refused(t, moveTo(arnOf(t, c, plain), arnOf(t, c, q.sources[0]), 0), "a plain source")
		requireCounts(t, c, plain, counts{visible: 1}, "the plain queue")
	})
	t.Run("a missing source", func(t *testing.T) {
		// Assumption: a missing queue is ResourceNotFoundException.
		requireNotFound(t, errOf(c.StartMessageMoveTask(t.Context(),
			moveTo(queueARN("missing"), "", 0))))
		requireNotFound(t, errOf(c.ListMessageMoveTasks(t.Context(),
			&sqs.ListMessageMoveTasksInput{SourceArn: aws.String(queueARN("missing"))})))
	})
	t.Run("a missing destination", func(t *testing.T) {
		// Assumption: as for a missing source.
		err := errOf(c.StartMessageMoveTask(t.Context(), moveTo(q.dlqARN, queueARN("gone"), 0)))
		requireNotFound(t, err)
		requireCounts(t, c, q.dlq, counts{visible: 1}, "the DLQ")
	})
	t.Run("a destination of the other queue type", func(t *testing.T) {
		// "you can also configure any other queue as the redrive destination
		// if both queues are the same type" (DLQ redrive guide).
		fifo := createFIFO(t, c, "dest.fifo", nil)
		refused(t, moveTo(q.dlqARN, arnOf(t, c, fifo), 0), "a FIFO destination")
	})
	t.Run("a rate above 500", func(t *testing.T) {
		// "The maximum value for messages per second is 500."
		refused(
			t,
			moveTo(q.dlqARN, arnOf(t, c, q.sources[0]), 501),
			"MaxNumberOfMessagesPerSecond 501",
		)
	})
	t.Run("an invalid task handle", func(t *testing.T) {
		requireClientError(t, errOf(c.CancelMessageMoveTask(t.Context(),
			&sqs.CancelMessageMoveTaskInput{TaskHandle: aws.String("bogus")})), "a bogus handle")
	})
	t.Run("canceling a completed task", func(t *testing.T) {
		// Only a RUNNING task can be canceled (CancelMessageMoveTask).
		handle := startMove(t, c, moveTo(q.dlqARN, arnOf(t, c, q.sources[0]), 0))
		requireEqual(t, aws.ToString(waitMove(t, c, q.dlqARN).Status), moveCompleted, "Status")
		requireClientError(t, errOf(c.CancelMessageMoveTask(t.Context(),
			&sqs.CancelMessageMoveTaskInput{TaskHandle: aws.String(handle)})), "a completed task")
	})
}

// The Query protocol, after the API reference's "Using AWS query protocol"
// examples of StartMessageMoveTask and ListMessageMoveTasks.

type startMoveXML struct {
	responseMetadata
	TaskHandle string `xml:"StartMessageMoveTaskResult>TaskHandle"`
}

type moveTaskXML struct {
	ToMove        *int64 `xml:"ApproximateNumberOfMessagesToMove"`
	DestARN       string `xml:"DestinationArn"`
	SourceARN     string `xml:"SourceArn"`
	Status        string `xml:"Status"`
	TaskHandle    string `xml:"TaskHandle"`
	FailureReason string `xml:"FailureReason"`
	Moved         int64  `xml:"ApproximateNumberOfMessagesMoved"`
	Rate          int32  `xml:"MaxNumberOfMessagesPerSecond"`
	Started       int64  `xml:"StartedTimestamp"`
}

// listMovesXML reads the entries under either name: the API reference's
// example has Result elements, the SDK model a flattened list named
// ListMessageMoveTasksResultEntry. Where ResponseMetadata sits differs
// too (inside the result in the example), so it is not asserted.
type listMovesXML struct {
	Result []moveTaskXML `xml:"ListMessageMoveTasksResult>Result"`
	Entry  []moveTaskXML `xml:"ListMessageMoveTasksResult>ListMessageMoveTasksResultEntry"`
}

func (l listMovesXML) entries() []moveTaskXML {
	return append(l.Result, l.Entry...)
}

// queryWaitMove polls Query ListMessageMoveTasks for sourceARN until its
// most recent task ends.
func queryWaitMove(t *testing.T, srv *server, sourceARN string) moveTaskXML {
	t.Helper()
	deadline := time.Now().Add(moveBound)
	for {
		var got listMovesXML
		decodeXML(t, query(t, srv, "/", url.Values{
			"Action": {"ListMessageMoveTasks"}, "SourceArn": {sourceARN}, "MaxResults": {"5"},
		}), "ListMessageMoveTasksResponse", &got)
		entries := got.entries()
		requireEqual(t, len(entries), 1, "Query-listed tasks")
		if s := entries[0].Status; s == moveCompleted || s == moveCancelled || s == moveFailed {
			return entries[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("the Query-listed task did not end within %v: %+v", moveBound, entries[0])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestQueryProtocolMoveTasks(t *testing.T) {
	c, srv := newClient(t)
	q := withDLQ(t, c, "MyDeadLetterQueue", "MySourceQueue")
	destARN := arnOf(t, c, q.sources[0])
	fillDLQ(t, c, q.dlq, 3)

	before := time.Now()
	var started startMoveXML
	decodeXML(t, query(t, srv, "/", url.Values{
		"Action":                       {"StartMessageMoveTask"},
		"SourceArn":                    {q.dlqARN},
		"DestinationArn":               {destARN},
		"MaxNumberOfMessagesPerSecond": {"10"},
	}), "StartMessageMoveTaskResponse", &started)
	started.require(t, "StartMessageMoveTask")
	if started.TaskHandle == "" {
		t.Fatal("StartMessageMoveTask: no TaskHandle")
	}

	e := queryWaitMove(t, srv, q.dlqARN)
	requireEqual(t, e.Status, moveCompleted, "Status")
	requireEqual(t, e.Moved, int64(3), "ApproximateNumberOfMessagesMoved")
	requireEqual(t, e.SourceARN, q.dlqARN, "SourceArn")
	requireEqual(t, e.DestARN, destARN, "DestinationArn")
	requireEqual(t, e.Rate, int32(10), "MaxNumberOfMessagesPerSecond")
	requireEqual(t, e.TaskHandle, "", "TaskHandle of a completed task")
	requireStartedBetween(t, e.Started, before, time.Now())
	requireCounts(t, c, q.sources[0], counts{visible: 3}, "the destination")

	// Canceling the completed task is refused with an ErrorResponse.
	res := query(t, srv, "/", url.Values{
		"Action": {"CancelMessageMoveTask"}, "TaskHandle": {started.TaskHandle},
	})
	if res.Status < 400 || res.Status > 499 {
		t.Fatalf("cancel a completed task: status %d, want 4xx: %s", res.Status, res.Body)
	}
	requireXMLRoot(t, res.Body, "ErrorResponse")
	var ex errorXML
	if err := xml.Unmarshal(res.Body, &ex); err != nil || ex.Code == "" ||
		ex.Code == "InvalidAction" {
		t.Fatalf("ErrorResponse without an Error/Code (%v): %s", err, res.Body)
	}
}

// Permissions. The DLQ redrive guide, "Configuring queue permissions for
// dead-letter queue redrive": to start a redrive, "sqs:StartMessageMoveTask,
// sqs:ReceiveMessage, sqs:DeleteMessage, and sqs:GetQueueAttributes of the
// dead-letter queue" and "sqs:SendMessage of the destination queue"; to
// cancel one, sqs:CancelMessageMoveTask, sqs:ReceiveMessage,
// sqs:DeleteMessage and sqs:GetQueueAttributes of the dead-letter queue; to
// show a move status, sqs:ListMessageMoveTasks and sqs:GetQueueAttributes
// of the dead-letter queue.

var (
	startMoveActions = []string{
		"sqs:StartMessageMoveTask", "sqs:ReceiveMessage", "sqs:DeleteMessage",
		"sqs:GetQueueAttributes",
	}
	cancelMoveActions = []string{
		"sqs:CancelMessageMoveTask", "sqs:ReceiveMessage", "sqs:DeleteMessage",
		"sqs:GetQueueAttributes",
	}
	listMoveActions = []string{"sqs:ListMessageMoveTasks", "sqs:GetQueueAttributes"}
)

// movePolicy allows dlqActions on the dead-letter queue and destActions on
// the destination.
func movePolicy(dlqARN string, dlqActions []string, destARN string, destActions []string) string {
	stmts := []map[string]any{{"Effect": "Allow", "Action": dlqActions, "Resource": dlqARN}}
	if len(destActions) > 0 {
		stmts = append(stmts, map[string]any{
			"Effect": "Allow", "Action": destActions, "Resource": destARN,
		})
	}
	b, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": stmts})
	return string(b)
}

// union is the actions of every list, each once.
func union(lists ...[]string) []string {
	var out []string
	for _, l := range lists {
		for _, a := range l {
			if !slices.Contains(out, a) {
				out = append(out, a)
			}
		}
	}
	return out
}

func without(actions []string, drop string) []string {
	return slices.DeleteFunc(slices.Clone(actions), func(a string) bool { return a == drop })
}

// moveSet is a dead-letter queue holding messages and a destination, made
// with the root key.
type moveSet struct {
	dlq, dlqARN, dest, destARN string
}

func (s *identityStack) moveSet(t *testing.T, prefix string, n int) moveSet {
	t.Helper()
	c := s.rootClient()
	q := withDLQ(t, c, prefix+"-dead", prefix+"-work")
	dest := createQueue(t, c, prefix+"-target", nil)
	fillDLQ(t, c, q.dlq, n)
	return moveSet{dlq: q.dlq, dlqARN: q.dlqARN, dest: dest, destARN: arnOf(t, c, dest)}
}

func TestMoveTaskPermissions(t *testing.T) {
	s := startIdentityStack(t)
	t.Run("the documented permissions allow a redrive", s.testMoveAllowed)
	t.Run("without sqs:StartMessageMoveTask", s.testMoveWithoutStart)
	t.Run("without sqs:SendMessage on the destination", s.testMoveWithoutSend)
	t.Run("without sqs:ListMessageMoveTasks", s.testMoveWithoutList)
	t.Run("without sqs:CancelMessageMoveTask", s.testMoveWithoutCancel)
}

func (s *identityStack) testMoveAllowed(t *testing.T) {
	m := s.moveSet(t, "allowed", 2)
	doc := movePolicy(m.dlqARN, union(startMoveActions, listMoveActions, cancelMoveActions),
		m.destARN, []string{"sqs:SendMessage"})
	c, _ := s.policyUser(t, "redriver", doc)
	startMove(t, c, moveTo(m.dlqARN, m.destARN, 0))
	requireEnded(t, waitMove(t, c, m.dlqARN), moveCompleted, 2, m.dlqARN, m.destARN, 0)
	requireCounts(t, s.rootClient(), m.dest, counts{visible: 2}, "the destination")

	// Canceling needs only its own permissions on the dead-letter queue.
	fillDLQ(t, s.rootClient(), m.dlq, 10)
	canceler, _ := s.policyUser(t, "canceler",
		movePolicy(m.dlqARN, union(cancelMoveActions, listMoveActions), "", nil))
	handle := startMove(t, c, moveTo(m.dlqARN, m.destARN, 1))
	_, err := canceler.CancelMessageMoveTask(t.Context(),
		&sqs.CancelMessageMoveTaskInput{TaskHandle: aws.String(handle)})
	requireNoErr(t, err, "cancel with the documented permissions")
	requireEqual(t, aws.ToString(waitMove(t, canceler, m.dlqARN).Status), moveCancelled, "Status")
}

func (s *identityStack) testMoveWithoutStart(t *testing.T) {
	m := s.moveSet(t, "nostart", 2)
	doc := movePolicy(m.dlqARN, union(without(startMoveActions, "sqs:StartMessageMoveTask"),
		listMoveActions), m.destARN, []string{"sqs:SendMessage"})
	c, _ := s.policyUser(t, "nostart", doc)
	err := errOf(c.StartMessageMoveTask(t.Context(), moveTo(m.dlqARN, m.destARN, 0)))
	requireAccessDenied(t, err, userARN("nostart"), "sqs:StartMessageMoveTask", m.dlqARN, false)
	requireCounts(t, s.rootClient(), m.dlq, counts{visible: 2}, "the DLQ")
	requireCounts(t, s.rootClient(), m.dest, counts{}, "the destination")
}

func (s *identityStack) testMoveWithoutSend(t *testing.T) {
	// Whether SQS refuses the start or fails the task is not documented.
	// This service refuses the start: the caller needs sqs:SendMessage on
	// the destination queue (DLQ redrive guide), evaluated before the task
	// starts, so no task is created and nothing reaches the destination.
	m := s.moveSet(t, "nosend", 2)
	doc := movePolicy(m.dlqARN, union(startMoveActions, listMoveActions), "", nil)
	c, _ := s.policyUser(t, "nosend", doc)
	err := errOf(c.StartMessageMoveTask(t.Context(), moveTo(m.dlqARN, m.destARN, 0)))
	requireAccessDenied(t, err, userARN("nosend"), "sqs:SendMessage", m.destARN, false)
	requireEqual(t, len(listMoves(t, c, m.dlqARN, aws.Int32(10))), 0, "tasks after the refusal")
	requireCounts(t, s.rootClient(), m.dlq, counts{visible: 2}, "the DLQ")
	requireCounts(t, s.rootClient(), m.dest, counts{}, "the destination")
}

func (s *identityStack) testMoveWithoutList(t *testing.T) {
	m := s.moveSet(t, "nolist", 1)
	doc := movePolicy(m.dlqARN, startMoveActions, m.destARN, []string{"sqs:SendMessage"})
	c, _ := s.policyUser(t, "nolist", doc)
	err := errOf(c.ListMessageMoveTasks(t.Context(),
		&sqs.ListMessageMoveTasksInput{SourceArn: aws.String(m.dlqARN)}))
	requireAccessDenied(t, err, userARN("nolist"), "sqs:ListMessageMoveTasks", m.dlqARN, false)
}

func (s *identityStack) testMoveWithoutCancel(t *testing.T) {
	// Assumption: the resource of CancelMessageMoveTask is the task's
	// dead-letter queue, whose permissions the guide lists for it. At one
	// message a second, 60 messages keep the task running for a minute,
	// far longer than the refused cancel and the check after it take.
	m := s.moveSet(t, "nocancel", 60)
	root := s.rootClient()
	doc := movePolicy(m.dlqARN, union(without(cancelMoveActions, "sqs:CancelMessageMoveTask"),
		listMoveActions), "", nil)
	c, _ := s.policyUser(t, "nocancel", doc)
	handle := startMove(t, root, moveTo(m.dlqARN, m.destARN, 1))
	err := errOf(c.CancelMessageMoveTask(t.Context(),
		&sqs.CancelMessageMoveTaskInput{TaskHandle: aws.String(handle)}))
	requireAccessDenied(t, err, userARN("nocancel"), "sqs:CancelMessageMoveTask", m.dlqARN, false)
	requireEqual(t, aws.ToString(latestMove(t, root, m.dlqARN).Status), moveRunning,
		"Status after the refused cancel")

	_, err = root.CancelMessageMoveTask(t.Context(),
		&sqs.CancelMessageMoveTaskInput{TaskHandle: aws.String(handle)})
	requireNoErr(t, err, "cancel with the root key")
	requireEqual(t, aws.ToString(waitMove(t, root, m.dlqARN).Status), moveCancelled, "Status")
}
