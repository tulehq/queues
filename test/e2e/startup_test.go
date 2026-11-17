//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gasmod/gas/config"

	"tule.dev/queues/internal/app"
)

func TestProbesNeedNoCredentials(t *testing.T) {
	srv := startServer(t, nil)
	for _, path := range []string{"/_health", "/_ready"} {
		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			srv.URL+path,
			nil,
		)
		requireNoErr(t, err, "build request")
		res, err := srv.Client().Do(req)
		requireNoErr(t, err, "probe "+path)
		_ = res.Body.Close()
		requireEqual(t, res.StatusCode, http.StatusOK, "unsigned "+path)
	}
}

func TestUnsignedRequestIsRejected(t *testing.T) {
	// MissingAuthenticationToken, HTTP 403, for a request with no signature
	// (SQS API reference, Common Errors).
	srv := startServer(t, nil)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/", strings.NewReader(`{"QueueNamePrefix":""}`))
	requireNoErr(t, err, "build request")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")

	res, err := srv.Client().Do(req)
	requireNoErr(t, err, "unsigned request")
	defer func() { _ = res.Body.Close() }()
	requireEqual(t, res.StatusCode, http.StatusForbidden, "unsigned request status")
	requireEqual(
		t,
		errorType(t, res.Body),
		"MissingAuthenticationToken",
		"unsigned request error type",
	)
}

func TestWrongCredentialsAreRejected(t *testing.T) {
	// InvalidClientTokenId (403, Common Errors) for an unknown key, and
	// SignatureDoesNotMatch for the right key with the wrong secret.
	srv := startServer(t, nil)
	for _, tt := range []struct {
		name, key, secret, code string
	}{
		{name: "unknown access key", key: "other", secret: "test", code: "InvalidClientTokenId"},
		{name: "wrong secret", key: "test", secret: "other", code: "SignatureDoesNotMatch"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := clientFor(srv, func(o *sqs.Options) {
				o.Credentials = credentials.NewStaticCredentialsProvider(tt.key, tt.secret, "")
			})
			err := errOf(
				c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("nope")}),
			)
			requireCode(t, err, tt.code)
			requireStatus(t, err, http.StatusForbidden)
		})
	}
	requireQueueMissing(t, clientFor(srv), "nope")
}

func TestUnknownOperation(t *testing.T) {
	// InvalidAction, 400: "The action or operation requested is invalid"
	// (SQS API reference, Common Errors).
	srv := startServer(t, nil)
	c := clientFor(srv)
	err := errOf(c.ListQueues(t.Context(), &sqs.ListQueuesInput{}, func(o *sqs.Options) {
		o.APIOptions = append(o.APIOptions, setTargetHeader("AmazonSQS.Nonsense"))
	}))
	requireCode(t, err, "InvalidAction")
	requireStatus(t, err, http.StatusBadRequest)
}

func TestDataSurvivesARestart(t *testing.T) {
	// The design: one SQLite store at TULE_Store_Path.
	env := map[string]string{"TULE_Store_Path": filepath.Join(t.TempDir(), "kept.db")}
	srv := startServer(t, env)
	c := clientFor(srv)

	u, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName:  aws.String("kept"),
		Attributes: map[string]string{"VisibilityTimeout": "300", "DelaySeconds": "0"},
		Tags:       map[string]string{"env": "dev"},
	})
	requireNoErr(t, err, "create queue")
	kept := aws.ToString(u.QueueUrl)
	fifo := createFIFO(t, c, "kept.fifo", nil)
	attrsBefore := queueAttrs(t, c, kept, "All")

	visible := send(t, c, kept, "still here")
	send(t, c, kept, "in flight")
	msgs := receive(t, c, kept, 2)
	requireEqual(t, len(msgs), 2, "received before the restart")
	for _, m := range msgs {
		if aws.ToString(m.MessageId) == aws.ToString(visible.MessageId) {
			changeVisibility(t, c, kept, m, 0)
		}
	}
	sendFIFO(t, c, fifo, "once", "g", "dedup")
	srv.Stop(t)

	srv = startServer(t, env)
	c = clientFor(srv)
	// The URL has the new port; GetQueueUrl answers it.
	kept, fifo = srv.queueURL("kept"), srv.queueURL("kept.fifo")
	requireEqual(
		t,
		createQueue(
			t,
			c,
			"kept",
			map[string]string{"VisibilityTimeout": "300", "DelaySeconds": "0"},
		),
		kept,
		"create again with the same attributes after the restart",
	)

	after := queueAttrs(t, c, kept, "All")
	for _, k := range []string{"VisibilityTimeout", "QueueArn", "CreatedTimestamp", "MessageRetentionPeriod"} {
		requireEqual(t, after[k], attrsBefore[k], k+" after the restart")
	}
	requireTags(t, c, kept, map[string]string{"env": "dev"}, "tags after the restart")
	requireCounts(t, c, kept, counts{visible: 1, notVisible: 1}, "counts after the restart")
	m := receiveOne(t, c, kept)
	requireEqual(
		t,
		aws.ToString(m.MessageId),
		aws.ToString(visible.MessageId),
		"the visible message",
	)
	requireEqual(t, receiveCount(t, m), 2, "receive count after the restart")

	// The deduplication record survives too.
	sendFIFO(t, c, fifo, "once", "g", "dedup")
	requireEqual(t, len(receive(t, c, fifo, 10)), 1, "FIFO messages after a duplicate send")
}

func errorType(t *testing.T, body io.Reader) string {
	t.Helper()
	var envelope struct {
		Type string `json:"__type"`
	}
	if err := json.NewDecoder(body).Decode(&envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	// AWS JSON allows a namespace prefix ("ns#Code").
	if i := strings.LastIndex(envelope.Type, "#"); i >= 0 {
		return envelope.Type[i+1:]
	}
	return envelope.Type
}

func TestStartupQueues(t *testing.T) {
	env := map[string]string{
		"TULE_Store_Path":   filepath.Join(t.TempDir(), "startup.db"),
		"TULE_Store_Queues": "jobs, events.fifo",
	}
	srv := startServer(t, env)
	c := clientFor(srv)

	// Both are there before any CreateQueue, with the defaults; the .fifo
	// name is a FIFO queue.
	jobs, events := queueURLOf(t, c, "jobs"), queueURLOf(t, c, "events.fifo")
	requireEqual(t, queueAttrs(t, c, jobs, "VisibilityTimeout")["VisibilityTimeout"], "30",
		"startup queue's default visibility timeout")
	_, isFIFO := queueAttrs(t, c, jobs, "FifoQueue")["FifoQueue"]
	requireEqual(t, isFIFO, false, "jobs is a standard queue")
	requireEqual(t, queueAttrs(t, c, events, "FifoQueue")["FifoQueue"], "true",
		"events.fifo is a FIFO queue")
	// The same as a FIFO queue made through the API, but for its name.
	made := queueAttrs(t, c, createFIFO(t, c, "made.fifo", nil), "All")
	got := queueAttrs(t, c, events, "All")
	for _, a := range []string{"QueueArn", "CreatedTimestamp", "LastModifiedTimestamp"} {
		delete(made, a)
		delete(got, a)
	}
	requireEqual(t, reflect.DeepEqual(got, made), true, "startup FIFO queue's attributes")
	_, err := c.DeleteQueue(
		t.Context(),
		&sqs.DeleteQueueInput{QueueUrl: aws.String(srv.queueURL("made.fifo"))},
	)
	requireNoErr(t, err, "delete made.fifo")
	sendFIFO(t, c, events, "first", "g", "d1")
	requireEqual(t, aws.ToString(receiveOne(t, c, events).Body), "first", "FIFO round trip")

	// A queue that exists is kept as it is: its attributes and its messages.
	_, err = c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(jobs),
		Attributes: map[string]string{"VisibilityTimeout": "120"},
	})
	requireNoErr(t, err, "set visibility timeout")
	send(t, c, jobs, "kept")
	srv.Stop(t)

	srv = startServer(t, env)
	c = clientFor(srv)
	jobs = queueURLOf(t, c, "jobs")
	requireEqual(t, queueAttrs(t, c, jobs, "VisibilityTimeout")["VisibilityTimeout"], "120",
		"visibility timeout after a restart")
	requireEqual(t, aws.ToString(receiveOne(t, c, jobs).Body), "kept", "message after a restart")

	list, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{})
	requireNoErr(t, err, "list queues")
	requireEqual(t, len(list.QueueUrls), 2, "queues after a restart")
}

func TestStartupQueueDeletedRecentlyFailsTheStart(t *testing.T) {
	env := map[string]string{
		"TULE_Store_Path":   filepath.Join(t.TempDir(), "deleted.db"),
		"TULE_Store_Queues": "jobs",
	}
	srv := startServer(t, env)
	c := clientFor(srv)
	_, err := c.DeleteQueue(
		t.Context(),
		&sqs.DeleteQueueInput{QueueUrl: aws.String(queueURLOf(t, c, "jobs"))},
	)
	requireNoErr(t, err, "delete jobs")
	srv.Stop(t)

	// Within the 60 seconds SQS keeps a deleted queue's name, the start
	// fails, naming the queue and the reason.
	setEnv(t, env)
	cfg, err := app.LoadConfigFrom(config.NewEnvProvider(
		config.WithEnvSeparator("_"),
		config.WithEnvPrefix("TULE_"),
	))
	requireNoErr(t, err, "load config")
	a, err := app.New(cfg)
	requireNoErr(t, err, "new app")
	err = a.Start(context.Background())
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err == nil || !strings.Contains(err.Error(), `"jobs"`) ||
		!strings.Contains(err.Error(), "deleted less than 60 seconds ago") {
		t.Fatalf(
			"start after deleting a startup queue = %v, want the deleted-recently failure",
			err,
		)
	}
}

func queueURLOf(t *testing.T, c *sqs.Client, name string) string {
	t.Helper()
	out, err := c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	requireNoErr(t, err, "get queue URL of "+name)
	return aws.ToString(out.QueueUrl)
}
