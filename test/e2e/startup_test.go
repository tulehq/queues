//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
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
