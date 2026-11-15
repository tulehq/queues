//go:build e2e

package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"tule.dev/queues/servicesend"
)

// These tests send messages through the service-send endpoint, as a Tule
// service sends one as an AWS service principal, with the servicesend
// client and with raw signed calls, on the real server.

const (
	s3Service = "s3.amazonaws.com"
	// bucket is the bucket the events come from, and bucketARN its ARN as S3
	// supplies it as aws:SourceArn: no region and no account.
	bucket    = "photos"
	bucketARN = "arn:aws:s3:::" + bucket
)

// s3NotificationPolicy is the queue policy the S3 User Guide gives for
// event notifications ("Granting permissions to publish event notification
// messages to a destination"), for the queue name and the bucket.
func s3NotificationPolicy(name string) string {
	return s3NotificationPolicyFor(name, bucket)
}

// s3NotificationPolicyFor is s3NotificationPolicy for the bucket b.
func s3NotificationPolicyFor(name, b string) string {
	return `{
  "Version": "2012-10-17",
  "Id": "example-ID",
  "Statement": [` + s3GrantStatement(name, b) + `]
}`
}

// s3GrantStatement is the statement of s3NotificationPolicyFor.
func s3GrantStatement(name, b string) string {
	return `{
    "Sid": "example-statement-ID",
    "Effect": "Allow",
    "Principal": {"Service": "s3.amazonaws.com"},
    "Action": ["SQS:SendMessage"],
    "Resource": "` + queueARN(name) + `",
    "Condition": {
      "ArnLike": {"aws:SourceArn": "arn:aws:s3:*:*:` + b + `"},
      "StringEquals": {"aws:SourceAccount": "` + defaultAccountID + `"}
    }
  }`
}

// serviceSender returns a servicesend client for srv, signing with creds.
func serviceSender(t *testing.T, srv *server, creds aws.Credentials) *servicesend.Client {
	t.Helper()
	c, err := servicesend.New(servicesend.Config{
		HTTPClient:      srv.Client(),
		Endpoint:        srv.URL,
		Region:          region,
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
	})
	requireNoErr(t, err, "new servicesend client")
	return c
}

func rootSender(t *testing.T, srv *server) *servicesend.Client {
	t.Helper()
	return serviceSender(t, srv, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"})
}

// asBucket are the options of a send for the bucket.
func asBucket() []servicesend.Option {
	return []servicesend.Option{
		servicesend.WithSourceArn(bucketARN),
		servicesend.WithSourceAccount(defaultAccountID),
	}
}

// notifiedQueue creates the queue name with attrs and the S3 notification
// policy, and returns its URL.
func notifiedQueue(t *testing.T, c *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()
	all := map[string]string{"Policy": s3NotificationPolicy(name)}
	for k, v := range attrs {
		all[k] = v
	}
	return createQueue(t, c, name, all)
}

func requireIs(t *testing.T, err, want error, what string) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: got %v, want %v", what, err, want)
	}
}

// requireSendError fails unless err is a *servicesend.Error with the status
// and code.
func requireSendError(t *testing.T, err error, status int, code, what string) {
	t.Helper()
	var e *servicesend.Error
	if !errors.As(err, &e) || e.Status != status || e.Code != code {
		t.Fatalf("%s: got %v, want %d %s", what, err, status, code)
	}
}

// rawServiceSend posts body to the endpoint, signed with creds for service,
// and returns the status, the request ID and the error document.
func rawServiceSend(
	t *testing.T, srv *server, creds aws.Credentials, service string, body []byte,
) (int, string, servicesend.ErrorResponse) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		srv.URL+servicesend.Path, bytes.NewReader(body))
	requireNoErr(t, err, "build the request")
	sum := sha256.Sum256(body)
	requireNoErr(t, v4.NewSigner().SignHTTP(t.Context(), creds, req,
		hex.EncodeToString(sum[:]), service, region, time.Now()), "sign the request")
	resp, err := srv.Client().Do(req)
	requireNoErr(t, err, "call the endpoint")
	defer func() { _ = resp.Body.Close() }()
	var e servicesend.ErrorResponse
	requireNoErr(t, json.NewDecoder(resp.Body).Decode(&e), "decode the answer")
	return resp.StatusCode, resp.Header.Get(servicesend.RequestIDHeader), e
}

// receiveWithSender receives one message of the queue with its SenderId.
func receiveWithSender(t *testing.T, c *sqs.Client, u string) types.Message {
	t.Helper()
	msgs := receiveInput(t, c, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(u),
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameSenderId,
		},
		MaxNumberOfMessages: 1,
	})
	if len(msgs) != 1 {
		t.Fatalf("received %d messages, want 1", len(msgs))
	}
	return msgs[0]
}

func TestServiceSendAllowedByTheS3NotificationPolicy(t *testing.T) {
	c, srv := newClient(t)
	u := notifiedQueue(t, c, "events", nil)
	// Characters JSON escapes, or HTML-escapes by default, and some outside
	// the BMP: the queue gets them as they were sent.
	body := `{"Records":[{"s3":{"object":{"key":"a+b/c%2Bd<e>&f"}}}]}` +
		" \"quoted\" \\ \t\r\n \u2028\u2029 \U0001F600 \uFFFD"

	// arn:aws:s3:*:*:<bucket> matches arn:aws:s3:::<bucket>: each * matches
	// an empty segment.
	res, err := rootSender(t, srv).SendMessage(t.Context(), queueARN("events"), s3Service, body,
		asBucket()...)
	requireNoErr(t, err, "send as the service")
	if res.MessageID == "" || res.RequestID == "" {
		t.Fatalf("result = %+v", res)
	}

	// The SDK checks the body's digest against MD5OfBody.
	m := receiveWithSender(t, c, u)
	requireEqual(t, aws.ToString(m.Body), body, "the body received")
	requireEqual(t, aws.ToString(m.MessageId), res.MessageID, "the message ID")
	requireEqual(t, aws.ToString(m.MD5OfBody), res.MD5OfMessageBody, "the body's digest")
	requireEqual(t, m.Attributes[string(types.MessageSystemAttributeNameSenderId)], s3Service,
		"SenderId")

	// The inspection API shows it too.
	items := inspectorFor(srv).allMessages(t, "events", nil)
	requireEqual(t, len(items), 1, "inspected messages")
	requireEqual(t, items[0].Body, body, "the inspected body")
	requireEqual(t, items[0].Attributes["SenderId"], s3Service, "the inspected SenderId")
}

// TestServiceSendAcceptsAThreeCharacterBucket checks that the ARN of a
// bucket of the shortest name S3 allows, arn:aws:s3:::abc, is a SourceArn
// the endpoint takes and the S3 notification policy matches.
func TestServiceSendAcceptsAThreeCharacterBucket(t *testing.T) {
	c, srv := newClient(t)
	u := createQueue(t, c, "short", map[string]string{
		"Policy": s3NotificationPolicyFor("short", "abc"),
	})
	_, err := rootSender(t, srv).SendMessage(t.Context(), queueARN("short"), s3Service,
		"from abc", servicesend.WithSourceArn("arn:aws:s3:::abc"),
		servicesend.WithSourceAccount(defaultAccountID))
	requireNoErr(t, err, "send for the bucket abc")
	requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), "from abc", "the body received")
}

// TestServiceSendMeasuresTheMessageNotTheRequest checks that the queue's
// MaximumMessageSize is held to the body's bytes, not to its escaped form
// in the request: 1 MiB of quotes, two bytes each in JSON, is a message of
// the limit.
func TestServiceSendMeasuresTheMessageNotTheRequest(t *testing.T) {
	c, srv := newClient(t)
	u := notifiedQueue(t, c, "big", nil)
	small := notifiedQueue(t, c, "small", map[string]string{"MaximumMessageSize": "1024"})
	sc := rootSender(t, srv)

	body := strings.Repeat(`"`, 1<<20)
	_, err := sc.SendMessage(t.Context(), queueARN("big"), s3Service, body, asBucket()...)
	requireNoErr(t, err, "send 1 MiB of quotes")
	requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), body, "the 1 MiB body")

	_, err = sc.SendMessage(t.Context(), queueARN("small"), s3Service,
		strings.Repeat(`"`, 1024), asBucket()...)
	requireNoErr(t, err, "send 1024 quotes to a 1024-byte queue")
	_, err = sc.SendMessage(t.Context(), queueARN("small"), s3Service,
		strings.Repeat("x", 1025), asBucket()...)
	requireIs(t, err, servicesend.ErrInvalidMessage, "a body over MaximumMessageSize")
	requireEqual(t, len(receive(t, c, small, 10)), 1, "messages in the small queue")
}

func TestServiceSendRefusals(t *testing.T) {
	c, srv := newClient(t)
	notifiedQueue(t, c, "events", nil)
	createQueue(t, c, "nopolicy", nil)
	createFIFO(t, c, "events.fifo", map[string]string{
		"Policy": s3NotificationPolicy("events.fifo"),
	})
	sc := rootSender(t, srv)
	ctx := t.Context()
	arn := queueARN("events")

	denied := map[string]func() error{
		"another service": func() error {
			return errOf(sc.SendMessage(ctx, arn, "sns.amazonaws.com", "x", asBucket()...))
		},
		"another bucket": func() error {
			return errOf(sc.SendMessage(ctx, arn, s3Service, "x",
				servicesend.WithSourceArn(bucketARN+"-other"),
				servicesend.WithSourceAccount(defaultAccountID)))
		},
		"another SourceAccount": func() error {
			return errOf(sc.SendMessage(ctx, arn, s3Service, "x",
				servicesend.WithSourceArn(bucketARN),
				servicesend.WithSourceAccount("111111111111")))
		},
		"no SourceArn or SourceAccount": func() error {
			return errOf(sc.SendMessage(ctx, arn, s3Service, "x"))
		},
		"no queue policy": func() error {
			return errOf(sc.SendMessage(ctx, queueARN("nopolicy"), s3Service, "x",
				asBucket()...))
		},
	}
	for what, call := range denied {
		requireIs(t, call(), servicesend.ErrAccessDenied, what)
	}

	for what, a := range map[string]string{
		"a missing queue":         queueARN("missing"),
		"another account's queue": "arn:aws:sqs:" + region + ":111111111111:events",
		"another region's queue":  "arn:aws:sqs:eu-west-1:" + defaultAccountID + ":events",
		"another partition":       "arn:aws-cn:sqs:" + region + ":" + defaultAccountID + ":events",
	} {
		_, err := sc.SendMessage(ctx, a, s3Service, "x", asBucket()...)
		requireIs(t, err, servicesend.ErrNotFound, what)
	}

	_, err := sc.SendMessage(ctx, queueARN("events.fifo"), s3Service, "x", asBucket()...)
	requireIs(t, err, servicesend.ErrInvalidMessage, "a FIFO queue, which needs a group")
	_, err = sc.SendMessage(ctx, arn, s3Service, "a\u0001b", asBucket()...)
	requireIs(t, err, servicesend.ErrInvalidMessage, "a character SQS does not take")
	_, err = sc.SendMessage(ctx, arn, s3Service, "", asBucket()...)
	requireIs(t, err, servicesend.ErrInvalidMessage, "an empty body")
	_, err = sc.SendMessage(ctx, "orders", s3Service, "x")
	requireSendError(t, err, http.StatusBadRequest, servicesend.CodeInvalidRequest,
		"a QueueArn that is no ARN")
	_, err = sc.SendMessage(ctx, arn, "not a service", "x")
	requireSendError(t, err, http.StatusBadRequest, servicesend.CodeInvalidRequest,
		"a malformed service principal")

	// Nothing refused was sent.
	requireEmpty(t, c, srv.queueURL("events"), "the queue after the refusals")
}

func TestServiceSendTakesTheRootKeyAlone(t *testing.T) {
	c, srv := newClient(t)
	notifiedQueue(t, c, "events", nil)
	root := aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}
	body := []byte(`{"QueueArn":"` + queueARN("events") + `",` +
		`"ServicePrincipal":"s3.amazonaws.com","MessageBody":"x"}`)

	status, id, e := rawServiceSend(t, srv, root, "lambda", body)
	if status != http.StatusForbidden || e.Code != "WrongService" || id == "" {
		t.Fatalf("signed for another service: %d %q %+v, want 403 WrongService", status, id, e)
	}
	status, _, e = rawServiceSend(t, srv, root, "sqs",
		bytes.Repeat([]byte(" "), 4<<20+1))
	if status != http.StatusRequestEntityTooLarge || e.Code != servicesend.CodeRequestTooLarge {
		t.Fatalf("an oversized body: %d %+v, want 413 RequestTooLarge", status, e)
	}
	status, _, e = rawServiceSend(t, srv, aws.Credentials{
		AccessKeyID: "test", SecretAccessKey: "wrong",
	}, "sqs", body)
	if status != http.StatusForbidden || e.Code != "SignatureMismatch" {
		t.Fatalf("a wrong secret: %d %+v, want 403 SignatureMismatch", status, e)
	}
	requireEmpty(t, c, srv.queueURL("events"), "the queue after the refusals")
}

// TestServiceSendRefusesIdentityIssuedKeys runs against a real identity: a
// user's key is refused as unknown however its policies allow it, and the
// root key identity shares with queues works.
func TestServiceSendRefusesIdentityIssuedKeys(t *testing.T) {
	s := startIdentityStack(t)
	c := s.rootClient()
	u := notifiedQueue(t, c, "events", nil)
	userKey := createUserKey(t, s.iam, "dev")
	putUserPolicy(t, s.iam, "dev", allowAllQueuesPolicy)

	_, err := serviceSender(t, s.srv, userKey).SendMessage(t.Context(), queueARN("events"),
		s3Service, "x", asBucket()...)
	requireSendError(t, err, http.StatusForbidden, "UnknownAccessKey", "an IAM user's key")

	_, err = serviceSender(t, s.srv, s.rootCreds).SendMessage(t.Context(), queueARN("events"),
		s3Service, "from the root key", asBucket()...)
	requireNoErr(t, err, "send with the root key")
	m := receiveWithSender(t, c, u)
	requireEqual(t, aws.ToString(m.Body), "from the root key", "the body received")
	requireEqual(t, m.Attributes[string(types.MessageSystemAttributeNameSenderId)], s3Service,
		"SenderId")
}

// TestServiceSendEvaluatesTheWholeQueuePolicy checks, through the endpoint,
// a Deny that overrides the S3 grant, an Allow of Principal "*" conditioned
// on aws:SourceArn, and an Allow conditioned on the queue's tags.
func TestServiceSendEvaluatesTheWholeQueuePolicy(t *testing.T) {
	c, srv := newClient(t)
	sc := rootSender(t, srv)
	ctx := t.Context()
	policyOf := func(statements string) map[string]string {
		return map[string]string{
			"Policy": `{"Version":"2012-10-17","Statement":[` + statements + `]}`,
		}
	}

	// The S3 grant, and a Deny of the same service on the same action.
	createQueue(t, c, "denied", policyOf(s3GrantStatement("denied", bucket)+`,{"Effect":"Deny",
		"Principal":{"Service":"s3.amazonaws.com"},"Action":"sqs:SendMessage",
		"Resource":"`+queueARN("denied")+`"}`))
	_, err := sc.SendMessage(ctx, queueARN("denied"), s3Service, "x", asBucket()...)
	requireIs(t, err, servicesend.ErrAccessDenied, "a Deny beside the grant")

	// Principal "*", for the bucket's ARN alone.
	anyU := createQueue(t, c, "any", policyOf(`{"Effect":"Allow","Principal":"*",
		"Action":"sqs:SendMessage","Resource":"`+queueARN("any")+`",
		"Condition":{"ArnEquals":{"aws:SourceArn":"`+bucketARN+`"}}}`))
	_, err = sc.SendMessage(ctx, queueARN("any"), s3Service, "from *", asBucket()...)
	requireNoErr(t, err, `Principal "*" for the bucket`)
	_, err = sc.SendMessage(ctx, queueARN("any"), "sns.amazonaws.com", "x",
		servicesend.WithSourceArn(bucketARN))
	requireNoErr(t, err, `Principal "*" for another service with the bucket's ARN`)
	_, err = sc.SendMessage(ctx, queueARN("any"), s3Service, "x",
		servicesend.WithSourceArn(bucketARN+"-other"))
	requireIs(t, err, servicesend.ErrAccessDenied, `Principal "*" for another bucket`)
	requireEqual(t, len(receive(t, c, anyU, 10)), 2, `messages Principal "*" allowed`)

	// An Allow on the queue's tag.
	tagged := createQueue(t, c, "tagged", policyOf(`{"Effect":"Allow",
		"Principal":{"Service":"s3.amazonaws.com"},"Action":"sqs:SendMessage",
		"Resource":"`+queueARN("tagged")+`",
		"Condition":{"StringEquals":{"aws:ResourceTag/events":"on"}}}`))
	_, err = sc.SendMessage(ctx, queueARN("tagged"), s3Service, "x", asBucket()...)
	requireIs(t, err, servicesend.ErrAccessDenied, "an untagged queue")
	_, err = c.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: aws.String(tagged), Tags: map[string]string{"events": "on"},
	})
	requireNoErr(t, err, "tag the queue")
	_, err = sc.SendMessage(ctx, queueARN("tagged"), s3Service, "tagged", asBucket()...)
	requireNoErr(t, err, "a tagged queue")
	requireEqual(t, aws.ToString(receiveOne(t, c, tagged).Body), "tagged", "the tagged body")
}
