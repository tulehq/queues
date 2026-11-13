//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// inspector is a client of the inspection API under /_queues/, which is not
// an AWS API: each request is signed SigV4 for service sqs.
type inspector struct {
	srv   *server
	creds aws.Credentials
}

func inspectorFor(srv *server) *inspector {
	return inspectorWith(srv, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"})
}

func inspectorWith(srv *server, creds aws.Credentials) *inspector {
	return &inspector{srv: srv, creds: creds}
}

// response is what the inspection API answered.
type response struct {
	Header http.Header
	Body   []byte
	Status int
}

// inspectedQueue is a listed queue: name, url, arn, the GetQueueAttributes
// All map, and the counts.
type inspectedQueue struct {
	Attributes map[string]string `json:"attributes"`
	Name       string            `json:"name"`
	URL        string            `json:"url"`
	ARN        string            `json:"arn"`
	Counts     inspectedCounts   `json:"counts"`
}

type inspectedCounts struct {
	Visible  int `json:"visible"`
	InFlight int `json:"inFlight"`
	Delayed  int `json:"delayed"`
}

// inspectedAttr is a message attribute: dataType, and stringValue or
// binaryValue (base64, which []byte decodes).
type inspectedAttr struct {
	StringValue *string `json:"stringValue"`
	DataType    string  `json:"dataType"`
	BinaryValue []byte  `json:"binaryValue"`
}

// inspectedMessage is a listed message.
type inspectedMessage struct {
	Attributes        map[string]string        `json:"attributes"`
	MessageAttributes map[string]inspectedAttr `json:"messageAttributes"`
	ID                string                   `json:"id"`
	Body              string                   `json:"body"`
	SentTime          string                   `json:"sentTime"`
	State             string                   `json:"state"`
	VisibleAt         string                   `json:"visibleAt"`
	Group             string                   `json:"group"`
	DeduplicationID   string                   `json:"deduplicationId"`
	SequenceNumber    string                   `json:"sequenceNumber"`
	ReceiveCount      int                      `json:"receiveCount"`
}

type messagePage struct {
	NextCursor string             `json:"nextCursor"`
	Items      []inspectedMessage `json:"items"`
}

// timeFormat is the shape of every time: RFC 3339, UTC, milliseconds.
var timeFormat = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func parseTime(t *testing.T, s, what string) time.Time {
	t.Helper()
	requireMatch(t, timeFormat, s, what)
	v, err := time.Parse(time.RFC3339Nano, s)
	requireNoErr(t, err, "parse "+what)
	return v
}

// do sends a signed request and returns the response.
func (in *inspector) do(t *testing.T, method, path string, q url.Values) response {
	t.Helper()
	req := in.request(t, method, path, q)
	sum := sha256.Sum256(nil)
	err := v4.NewSigner().SignHTTP(context.Background(), in.creds, req,
		hex.EncodeToString(sum[:]), "sqs", region, time.Now())
	requireNoErr(t, err, "sign "+method+" "+path)
	return in.send(t, req)
}

func (in *inspector) doUnsigned(t *testing.T, method, path string) response {
	t.Helper()
	return in.send(t, in.request(t, method, path, nil))
}

func (in *inspector) request(t *testing.T, method, path string, q url.Values) *http.Request {
	t.Helper()
	u := in.srv.URL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(context.Background(), method, u, http.NoBody)
	requireNoErr(t, err, "build "+method+" "+path)
	return req
}

func (in *inspector) send(t *testing.T, req *http.Request) response {
	t.Helper()
	res, err := in.srv.Client().Do(req)
	requireNoErr(t, err, req.Method+" "+req.URL.Path)
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	requireNoErr(t, err, "read "+req.Method+" "+req.URL.Path)
	return response{Status: res.StatusCode, Header: res.Header, Body: body}
}

// queues lists every queue: 200 {"items": [...]}, with no nextCursor.
func (in *inspector) queues(t *testing.T) []inspectedQueue {
	t.Helper()
	res := in.do(t, http.MethodGet, "/_queues/queues", nil)
	requireOK(t, res, "list queues")
	var shape map[string]json.RawMessage
	decode(t, res, &shape)
	if _, ok := shape["nextCursor"]; ok {
		t.Fatalf("the queue list has a nextCursor: %s", res.Body)
	}
	requireArray(t, shape["items"], "items")
	var p struct {
		Items []inspectedQueue `json:"items"`
	}
	decode(t, res, &p)
	return p.Items
}

// messages lists one page of a queue's messages.
func (in *inspector) messages(t *testing.T, queue string, q url.Values) messagePage {
	t.Helper()
	res := in.do(t, http.MethodGet, "/_queues/queues/"+url.PathEscape(queue)+"/messages", q)
	requireOK(t, res, "list messages of "+queue)
	var shape map[string]json.RawMessage
	decode(t, res, &shape)
	requireArray(t, shape["items"], "items")
	if raw, ok := shape["nextCursor"]; ok && string(bytes.TrimSpace(raw)) == `""` {
		t.Fatalf("nextCursor is present but empty, want it omitted: %s", res.Body)
	}
	var p messagePage
	decode(t, res, &p)
	for _, m := range p.Items {
		requireMessageShape(t, m)
	}
	return p
}

// allMessages lists every message of queue matching q, following the cursor.
func (in *inspector) allMessages(t *testing.T, queue string, q url.Values) []inspectedMessage {
	t.Helper()
	params := url.Values{}
	for k, v := range q {
		params[k] = v
	}
	var all []inspectedMessage
	for range 1000 {
		p := in.messages(t, queue, params)
		all = append(all, p.Items...)
		if p.NextCursor == "" {
			return all
		}
		params.Set("cursor", p.NextCursor)
	}
	t.Fatal("the cursor did not end after 1000 pages")
	return nil
}

// requireMessageShape checks what every listed message holds.
func requireMessageShape(t *testing.T, m inspectedMessage) {
	t.Helper()
	if m.ID == "" {
		t.Fatalf("listed message without an id: %+v", m)
	}
	parseTime(t, m.SentTime, "sentTime of "+m.ID)
	parseTime(t, m.VisibleAt, "visibleAt of "+m.ID)
	switch m.State {
	case "delayed", "visible", "inFlight":
	default:
		t.Fatalf("message %s has state %q", m.ID, m.State)
	}
}

func (in *inspector) purgeAll(t *testing.T) {
	t.Helper()
	requireNoContent(t, in.do(t, http.MethodDelete, "/_queues/messages", nil), "purge every queue")
}

func (in *inspector) deleteAll(t *testing.T) {
	t.Helper()
	requireNoContent(t, in.do(t, http.MethodDelete, "/_queues/queues", nil), "delete every queue")
}

func decode(t *testing.T, res response, v any) {
	t.Helper()
	if err := json.Unmarshal(res.Body, v); err != nil {
		t.Fatalf("decode %s: %v", res.Body, err)
	}
}

func requireArray(t *testing.T, raw json.RawMessage, what string) {
	t.Helper()
	if v := bytes.TrimSpace(raw); len(v) == 0 || v[0] != '[' {
		t.Fatalf("%s is %s, want an array", what, raw)
	}
}

func requireOK(t *testing.T, res response, what string) {
	t.Helper()
	if res.Status != http.StatusOK {
		t.Fatalf("%s: status %d, want 200: %s", what, res.Status, res.Body)
	}
}

func requireNoContent(t *testing.T, res response, what string) {
	t.Helper()
	if res.Status != http.StatusNoContent || len(res.Body) != 0 {
		t.Fatalf("%s: status %d with %d bytes, want 204 with no body: %s",
			what, res.Status, len(res.Body), res.Body)
	}
}

// requireAPIErrorBody fails unless res has the status, an X-Amzn-ErrorType
// header and a JSON body {code, message} with the same code, code itself.
func requireAPIErrorBody(t *testing.T, res response, status int, code, what string) {
	t.Helper()
	if res.Status != status {
		t.Fatalf("%s: status %d, want %d: %s", what, res.Status, status, res.Body)
	}
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(res.Body, &e); err != nil {
		t.Fatalf("%s: error body is not JSON: %v: %s", what, err, res.Body)
	}
	if e.Message == "" {
		t.Fatalf("%s: error body lacks a message: %s", what, res.Body)
	}
	header, _, _ := strings.Cut(res.Header.Get("X-Amzn-ErrorType"), ":")
	requireEqual(t, header, e.Code, what+": X-Amzn-ErrorType against the body's code")
	requireEqual(t, e.Code, code, what+": error code")
}
