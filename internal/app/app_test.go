package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

// fakeClock is a clock a test moves by hand.
type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// startApp starts the app on a store in dir, on clock when it is not nil,
// and returns an SDK client for its handler and the server's URL. The full
// server, over TCP, is what test/e2e drives; this only checks the wiring,
// the protocols and the store meet as the SDK expects.
func startApp(t *testing.T, dir string, clock *fakeClock) (*sqs.Client, string) {
	t.Helper()
	t.Setenv("TULE_Store_Path", filepath.Join(dir, "nested", "queues.db"))

	cfg, err := loadEnvConfig(t, "test", "test")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if clock != nil {
		cfg.SetClock(clock.Now)
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("new app: %v", err)
	}
	if err = a.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(func() {
		srv.Close()
		if sErr := a.Shutdown(context.Background()); sErr != nil {
			t.Errorf("shutdown: %v", sErr)
		}
	})

	return sqs.New(sqs.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String(srv.URL),
		Credentials:      credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:       srv.Client(),
		RetryMaxAttempts: 1,
	}), srv.URL
}

func createQueue(t *testing.T, c *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()
	out, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String(name), Attributes: attrs,
	})
	if err != nil {
		t.Fatalf("create queue %s: %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

func TestTheSDKSendsReceivesAndDeletesAMessage(t *testing.T) {
	ctx := t.Context()
	c, base := startApp(t, t.TempDir(), nil)

	url := createQueue(t, c, "orders", nil)
	if want := base + "/000000000000/orders"; url != want {
		t.Fatalf("queue URL = %s, want %s", url, want)
	}

	// The SDK checks MD5OfMessageBody against the body it sent and
	// received, so a wrong digest fails these calls.
	sent, err := c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("hello"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"kind": {DataType: aws.String("String"), StringValue: aws.String("greeting")},
		},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	got, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(url),
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{"All"},
	})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("received %d messages, want 1", len(got.Messages))
	}
	m := got.Messages[0]
	if aws.ToString(m.MessageId) != aws.ToString(sent.MessageId) ||
		aws.ToString(m.Body) != "hello" {
		t.Fatalf("received %+v, want the sent message", m)
	}
	if aws.ToString(m.MD5OfMessageAttributes) != aws.ToString(sent.MD5OfMessageAttributes) {
		t.Fatalf("attribute digest %s on receive, %s on send",
			aws.ToString(m.MD5OfMessageAttributes), aws.ToString(sent.MD5OfMessageAttributes))
	}
	if m.Attributes["ApproximateReceiveCount"] != "1" {
		t.Fatalf("attributes = %v, want ApproximateReceiveCount 1", m.Attributes)
	}
	if _, err = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	requireEmptyQueue(t, c, url, "arn:aws:sqs:us-east-1:000000000000:orders")
}

// requireEmptyQueue checks the queue's counts are all 0, and its ARN.
func requireEmptyQueue(t *testing.T, c *sqs.Client, url, arn string) {
	t.Helper()
	attrs, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{"All"},
	})
	if err != nil {
		t.Fatalf("get attributes: %v", err)
	}
	for _, name := range []string{
		"ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed",
	} {
		if attrs.Attributes[name] != "0" {
			t.Fatalf("%s = %q after the delete, want 0", name, attrs.Attributes[name])
		}
	}
	if attrs.Attributes["QueueArn"] != arn {
		t.Fatalf("QueueArn = %s", attrs.Attributes["QueueArn"])
	}
}

func TestTheSDKDecodesAQueueCompatibleError(t *testing.T) {
	c, _ := startApp(t, t.TempDir(), nil)

	_, err := c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("nope")})
	var missing *types.QueueDoesNotExist
	if !errors.As(err, &missing) {
		t.Fatalf("get queue url = %v, want QueueDoesNotExist", err)
	}
	// The x-amzn-query-error header gives the Query code, which the SDK
	// reports as the error code.
	if got := missing.ErrorCode(); got != "AWS.SimpleQueueService.NonExistentQueue" {
		t.Fatalf("error code = %s, want AWS.SimpleQueueService.NonExistentQueue", got)
	}
}

func TestAnUnservedOperationIsAnInvalidAction(t *testing.T) {
	c, _ := startApp(t, t.TempDir(), nil)
	url := createQueue(t, c, "moves", nil)

	_, err := c.ListMessageMoveTasks(t.Context(), &sqs.ListMessageMoveTasksInput{
		SourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:moves"),
	})
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "InvalidAction" {
		t.Fatalf("list message move tasks on %s = %v, want InvalidAction", url, err)
	}
}

func TestALongPollReturnsWhenAMessageIsSent(t *testing.T) {
	ctx := t.Context()
	c, _ := startApp(t, t.TempDir(), nil)
	url := createQueue(t, c, "polled", nil)

	start := time.Now()
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = c.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: aws.String(url), MessageBody: aws.String("late"),
		})
	}()
	got, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(url), WaitTimeSeconds: 10,
	})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("received %d messages, want 1", len(got.Messages))
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the long poll took %s, want it to return once the message was sent", took)
	}
}

func TestTheClockDrivesVisibility(t *testing.T) {
	ctx := t.Context()
	clock := &fakeClock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	c, _ := startApp(t, t.TempDir(), clock)
	url := createQueue(t, c, "orders", map[string]string{"VisibilityTimeout": "60"})

	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("m"),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	receive := func() int {
		got, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url)})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		return len(got.Messages)
	}
	if n := receive(); n != 1 {
		t.Fatalf("first receive got %d, want 1", n)
	}
	if n := receive(); n != 0 {
		t.Fatalf("receive while in flight got %d, want 0", n)
	}
	clock.Advance(61 * time.Second)
	if n := receive(); n != 1 {
		t.Fatalf("receive after the visibility timeout got %d, want 1", n)
	}
}

// TestAQueuePathMustHoldAQueueName sends a signed Query request to a path
// whose second segment can be no queue's name.
func TestAQueuePathMustHoldAQueueName(t *testing.T) {
	_, base := startApp(t, t.TempDir(), nil)

	body := "Action=PurgeQueue&Version=2012-11-05"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		base+"/000000000000/bad.name", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	sum := sha256.Sum256([]byte(body))
	if err = v4.NewSigner().SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, req,
		hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest ||
		!strings.Contains(string(got), "<Code>InvalidParameterValue</Code>") {
		t.Fatalf("answer = %d %s, want 400 InvalidParameterValue", resp.StatusCode, got)
	}
}
