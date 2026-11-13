//go:build e2e

package e2e

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithytraits "github.com/aws/smithy-go/traits"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/gasmod/gas"
	"github.com/gasmod/gas/config"

	"tule.dev/queues/internal/app"
)

// server is the app under test, served by the app's own *http.Server over a
// real TCP listener: the listener is created here because gas.App.Serve
// exposes neither the listener nor its port, and everything else is the
// server gas built from the bound configuration, reached over the network
// and shut down through gas.App.Stop.
type server struct {
	app      *gas.App
	listener net.Listener
	client   *http.Client
	served   chan error
	URL      string
	Addr     string
	stopped  bool
}

// clientTimeout bounds a test's own requests, so a hung server fails the test
// instead of the suite's timeout. It is longer than the longest long poll
// (20 seconds, ReceiveMessage WaitTimeSeconds).
const clientTimeout = 60 * time.Second

// region is the region every client signs for, the service's default
// (TULE_Queues_Region).
const region = "us-east-1"

// defaultAccountID is the account in queue URLs and ARNs when
// TULE_Queues_AccountID is unset.
const defaultAccountID = "000000000000"

// newClient starts the app on a fresh store and returns an SQS client
// pointed at it.
func newClient(t *testing.T) (*sqs.Client, *server) {
	t.Helper()
	srv := startServer(t, nil)
	return clientFor(srv), srv
}

// startServer starts the app on a fresh store, with env setting extra TULE_
// variables. It accepts the test/test credentials clientFor signs with unless
// env changes them.
func startServer(t *testing.T, env map[string]string) *server {
	t.Helper()
	return startServerWith(t, env, nil)
}

// startServerWith starts the app as startServer does, with now as the
// service clock when it is not nil.
func startServerWith(t *testing.T, env map[string]string, now func() time.Time) *server {
	t.Helper()

	setEnv(t, env)

	cfg, err := app.LoadConfigFrom(config.NewEnvProvider(
		config.WithEnvSeparator("_"),
		config.WithEnvPrefix("TULE_"),
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if now != nil {
		cfg.SetClock(now)
	}

	a, err := app.New(cfg)
	if err != nil {
		t.Fatalf("new app: %v", err)
	}

	// Start binds the configuration and initializes the services, so the
	// server built after it carries the configured timeouts.
	if err = a.Start(context.Background()); err != nil {
		t.Fatalf("start app: %v", err)
	}

	srv := serve(t, a)
	t.Cleanup(func() { srv.Stop(t) })
	return srv
}

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()

	t.Setenv("TULE_GasEnv", "testing")
	t.Setenv("TULE_Store_Path", filepath.Join(t.TempDir(), "queues.db"))
	t.Setenv("TULE_Auth_AccessKeyID", "test")
	t.Setenv("TULE_Auth_SecretAccessKey", "test")
	for k, v := range env {
		t.Setenv(k, v)
	}
}

// serve puts the app's own server on a loopback listener on a free port.
func serve(t *testing.T, a *gas.App) *server {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &server{
		app:      a,
		listener: listener,
		client:   &http.Client{Timeout: clientTimeout},
		served:   make(chan error, 1),
		Addr:     listener.Addr().String(),
		URL:      "http://" + listener.Addr().String(),
	}

	go func() {
		srv.served <- a.Server().Serve(listener)
	}()

	return srv
}

// Client returns an HTTP client for the server.
func (s *server) Client() *http.Client {
	return s.client
}

// Stop shuts the app down the way a signal does, through gas.App.Stop, and
// fails the test if the shutdown or the server itself reported an error. It
// is idempotent, so a test can stop the server early and let the cleanup run
// anyway.
func (s *server) Stop(t *testing.T) {
	t.Helper()

	if s.stopped {
		return
	}
	s.stopped = true

	// Close the client's spare connections first; the server would otherwise
	// wait for them to age out during the shutdown.
	s.client.CloseIdleConnections()

	if err := s.app.Stop(context.Background()); err != nil {
		t.Errorf("stop app: %v", err)
	}

	select {
	case err := <-s.served:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve: %v", err)
		}
	case <-time.After(clientTimeout):
		t.Error("serve did not return after shutdown")
	}
}

// queueURL is the URL the design gives a queue on srv with the default
// account: <scheme>://<host>/<account>/<name>, the host as the request
// reached the service.
func (s *server) queueURL(name string) string {
	return s.URL + "/" + defaultAccountID + "/" + name
}

// clientFor returns an SQS client for srv, with the test/test credentials
// unless opts change them. It does not retry, so a refused request fails at
// once and a test sees each request it makes exactly once.
func clientFor(srv *server, opts ...func(*sqs.Options)) *sqs.Client {
	return sqs.New(sqs.Options{
		Region:           region,
		BaseEndpoint:     aws.String(srv.URL),
		Credentials:      credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient:       srv.Client(),
		RetryMaxAttempts: 1,
	}, opts...)
}

// sqsClientWith returns an SQS client for srv signing with creds.
func sqsClientWith(srv *server, creds aws.Credentials) *sqs.Client {
	return clientFor(srv, func(o *sqs.Options) {
		o.Credentials = credentials.StaticCredentialsProvider{Value: creds}
	})
}

// createQueue creates a queue with attrs and returns its URL, failing the
// test on any error.
func createQueue(t *testing.T, c *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()

	out, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	requireNoErr(t, err, "create queue "+name)
	u := aws.ToString(out.QueueUrl)
	if u == "" {
		t.Fatalf("create queue %s returned no URL", name)
	}
	return u
}

// createFIFO creates a FIFO queue with attrs on top of FifoQueue=true.
func createFIFO(t *testing.T, c *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()
	all := map[string]string{"FifoQueue": "true"}
	for k, v := range attrs {
		all[k] = v
	}
	return createQueue(t, c, name, all)
}

// send sends body to the queue and returns the output, failing the test on
// any error.
func send(t *testing.T, c *sqs.Client, queueURL, body string) *sqs.SendMessageOutput {
	t.Helper()
	return sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	})
}

// sendInput sends in and returns the output, failing the test on any error
// and checking the body digest, which the SDK checks too when it is present
// (cust_checksum_validation.go) but not when it is missing.
func sendInput(t *testing.T, c *sqs.Client, in *sqs.SendMessageInput) *sqs.SendMessageOutput {
	t.Helper()

	out, err := c.SendMessage(t.Context(), in)
	requireNoErr(t, err, "send message")
	if aws.ToString(out.MessageId) == "" {
		t.Fatal("send message returned no message ID")
	}
	requireEqual(t, aws.ToString(out.MD5OfMessageBody), md5Hex(aws.ToString(in.MessageBody)),
		"MD5OfMessageBody")
	return out
}

// sendFIFO sends body to a FIFO queue in group with dedupID ("" sends none).
func sendFIFO(
	t *testing.T,
	c *sqs.Client,
	queueURL, body, group, dedupID string,
) *sqs.SendMessageOutput {
	t.Helper()
	in := &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String(body),
		MessageGroupId: aws.String(group),
	}
	if dedupID != "" {
		in.MessageDeduplicationId = aws.String(dedupID)
	}
	return sendInput(t, c, in)
}

// receive receives up to maxMessages messages with no wait, every system
// attribute and every message attribute, failing the test on any error.
func receive(t *testing.T, c *sqs.Client, queueURL string, maxMessages int32) []types.Message {
	t.Helper()
	return receiveInput(t, c, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(queueURL),
		MaxNumberOfMessages:         maxMessages,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{"All"},
		MessageAttributeNames:       []string{"All"},
	})
}

// receiveInput receives with in, failing the test on any error and checking
// each message's body digest and identifiers.
func receiveInput(t *testing.T, c *sqs.Client, in *sqs.ReceiveMessageInput) []types.Message {
	t.Helper()

	out, err := c.ReceiveMessage(t.Context(), in)
	requireNoErr(t, err, "receive message")
	for _, m := range out.Messages {
		if aws.ToString(m.MessageId) == "" || aws.ToString(m.ReceiptHandle) == "" {
			t.Fatalf("received a message without an ID or a receipt handle: %+v", m)
		}
		requireEqual(t, aws.ToString(m.MD5OfBody), md5Hex(aws.ToString(m.Body)), "MD5OfBody")
	}
	return out.Messages
}

// receiveOne receives exactly one message, failing otherwise.
func receiveOne(t *testing.T, c *sqs.Client, queueURL string) types.Message {
	t.Helper()
	msgs := receive(t, c, queueURL, 1)
	if len(msgs) != 1 {
		t.Fatalf("receive from %s: got %d messages, want 1", queueURL, len(msgs))
	}
	return msgs[0]
}

// requireEmpty fails unless a receive with no wait returns nothing.
func requireEmpty(t *testing.T, c *sqs.Client, queueURL, what string) {
	t.Helper()
	if msgs := receive(t, c, queueURL, 10); len(msgs) != 0 {
		t.Fatalf("%s: received %d messages, want none: first body %q",
			what, len(msgs), aws.ToString(msgs[0].Body))
	}
}

// deleteMessage deletes m, failing the test on any error.
func deleteMessage(t *testing.T, c *sqs.Client, queueURL string, m types.Message) {
	t.Helper()
	_, err := c.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: m.ReceiptHandle,
	})
	requireNoErr(t, err, "delete message")
}

// changeVisibility sets m's visibility timeout, failing on any error.
func changeVisibility(t *testing.T, c *sqs.Client, queueURL string, m types.Message, secs int32) {
	t.Helper()
	_, err := c.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: secs,
	})
	requireNoErr(t, err, "change message visibility")
}

// queueAttrs returns the named attributes of the queue ("All" for all).
func queueAttrs(t *testing.T, c *sqs.Client, queueURL string, names ...string) map[string]string {
	t.Helper()
	qn := make([]types.QueueAttributeName, 0, len(names))
	for _, n := range names {
		qn = append(qn, types.QueueAttributeName(n))
	}
	out, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: qn,
	})
	requireNoErr(t, err, "get queue attributes")
	return out.Attributes
}

// counts is a queue's approximate counts.
type counts struct {
	visible, notVisible, delayed int
}

// countsOf reads the three approximate counts of GetQueueAttributes.
func countsOf(t *testing.T, c *sqs.Client, queueURL string) counts {
	t.Helper()
	a := queueAttrs(t, c, queueURL, "ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed")
	return counts{
		visible:    atoi(t, a["ApproximateNumberOfMessages"], "ApproximateNumberOfMessages"),
		notVisible: atoi(t, a["ApproximateNumberOfMessagesNotVisible"], "NotVisible"),
		delayed:    atoi(t, a["ApproximateNumberOfMessagesDelayed"], "Delayed"),
	}
}

func requireCounts(t *testing.T, c *sqs.Client, queueURL string, want counts, what string) {
	t.Helper()
	if got := countsOf(t, c, queueURL); got != want {
		t.Fatalf("%s: counts %+v, want %+v", what, got, want)
	}
}

// receiveCount returns m's ApproximateReceiveCount system attribute.
func receiveCount(t *testing.T, m types.Message) int {
	t.Helper()
	v, ok := m.Attributes["ApproximateReceiveCount"]
	if !ok {
		t.Fatalf("message %s has no ApproximateReceiveCount: %v",
			aws.ToString(m.MessageId), m.Attributes)
	}
	return atoi(t, v, "ApproximateReceiveCount")
}

func atoi(t *testing.T, s, what string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("%s: %q is not an integer", what, s)
	}
	return n
}

// bodies returns the bodies of msgs, in order.
func bodies(msgs []types.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, aws.ToString(m.Body))
	}
	return out
}

// errOf returns the error of an SDK call and drops its output, for tests that
// call an operation only for the error it reports.
func errOf[T any](_ T, err error) error {
	return err
}

func requireNoErr(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func requireEqual[T comparable](t *testing.T, got, want T, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

func requireSameSet(t *testing.T, got, want []string, what string) {
	t.Helper()
	g, w := slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))
	if !slices.Equal(g, w) {
		t.Fatalf("%s: got %q, want %q", what, got, want)
	}
}

func requireSlice(t *testing.T, got, want []string, what string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: got %q, want %q", what, got, want)
	}
}

func requireMatch(t *testing.T, re *regexp.Regexp, got, what string) {
	t.Helper()
	if !re.MatchString(got) {
		t.Fatalf("%s: %q does not match %s", what, got, re)
	}
}

// requireAs fails unless err matches the modeled SDK error type T.
func requireAs[T error](t *testing.T, err error) {
	t.Helper()
	var want T
	if !errors.As(err, &want) {
		t.Fatalf("want error of type %T, got %v", want, err)
	}
}

// requireCode fails unless err is an API error with the given code. The
// code is what the SDK reports: for a modeled error the Query-compatible
// code of x-amzn-query-error when the service sends one, else __type.
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want API error %s, got %v", code, err)
	}
	if apiErr.ErrorCode() != code {
		t.Fatalf("want error code %s, got %s (%v)", code, apiErr.ErrorCode(), err)
	}
}

// requireModeled fails unless err is the modeled error T, with the HTTP
// status the SDK's schema gives it (httpError, schemas/schemas.go; 400 for
// the client errors that carry none), and the code the SDK reports for it
// is code: the schema's AWSQueryError code, which the SDK takes from
// x-amzn-query-error, or the shape name for the errors that have none.
func requireModeled[T error](t *testing.T, err error, code string) {
	t.Helper()
	requireAs[T](t, err)
	requireCode(t, err, code)
	requireStatus(t, err, modeledStatus[T](t))
}

// modeledStatus is the httpError status of the SDK's schema for T.
func modeledStatus[T error](t *testing.T) int {
	t.Helper()
	name := reflect.TypeFor[T]().Elem().Name()
	entry, ok := sqs.TypeRegistry.Entries["com.amazonaws.sqs#"+name]
	if !ok {
		t.Fatalf("no SDK schema for error %s", name)
	}
	if h, ok := smithy.SchemaTrait[*smithytraits.HTTPError](entry.Schema); ok {
		return h.Code
	}
	return http.StatusBadRequest
}

// skipValidation removes the SDK's client-side input validation, so a
// request the SDK would refuse itself reaches the service.
func skipValidation(o *sqs.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		_, err := stack.Initialize.Remove("OperationInputValidation")
		return err //nolint:wrapcheck // the stack's own error
	})
}

// requireInvalidParameter fails unless err is InvalidParameterValue, the
// common error for "an invalid or out-of-range value was supplied for the
// input parameter" (SQS API reference, Common Errors), with HTTP 400.
func requireInvalidParameter(t *testing.T, err error) {
	t.Helper()
	requireCode(t, err, "InvalidParameterValue")
	requireStatus(t, err, http.StatusBadRequest)
}

// requireRefused fails unless err is an API error the server answered with
// HTTP 400, for refusals whose code no AWS source pins.
func requireRefused(t *testing.T, err error) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want an API error, got %v", err)
	}
	requireStatus(t, err, http.StatusBadRequest)
}

// requireStatus fails unless err came with the given HTTP status.
func requireStatus(t *testing.T, err error, status int) {
	t.Helper()
	var re *awshttp.ResponseError
	if !errors.As(err, &re) {
		t.Fatalf("want an HTTP response error with status %d, got %v", status, err)
	}
	requireEqual(t, re.HTTPStatusCode(), status, "HTTP status")
}

// within fails unless f returns within d, and returns how long it took.
func within(t *testing.T, d time.Duration, what string, f func()) time.Duration {
	t.Helper()
	start := time.Now()
	f()
	took := time.Since(start)
	if took > d {
		t.Fatalf("%s took %v, want at most %v", what, took, d)
	}
	return took
}

// setTargetHeader replaces the operation header the SDK sends, the only way
// to ask for an operation no SQS client knows. It runs before signing, so
// the request stays properly signed.
func setTargetHeader(target string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
			"e2eSetTargetHeader",
			func(
				ctx context.Context,
				in middleware.FinalizeInput,
				next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set("X-Amz-Target", target)
				}
				return next.HandleFinalize(ctx, in)
			},
		), middleware.Before)
	}
}
