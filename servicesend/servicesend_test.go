package servicesend_test

import (
	"bytes"
	"crypto/md5" //nolint:gosec // SQS's digest of the body
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tule.dev/queues/servicesend"
)

const (
	keyID     = "test"
	secret    = "test"
	region    = "us-east-1"
	s3Service = "s3.amazonaws.com"
	queueARN  = "arn:aws:sqs:us-east-1:000000000000:events"
	bucketARN = "arn:aws:s3:::abc"
)

func client(t *testing.T, url string) *servicesend.Client {
	t.Helper()
	c, err := servicesend.New(servicesend.Config{
		Endpoint: url, Region: region, AccessKeyID: keyID, SecretAccessKey: secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec // SQS's digest of the body
	return hex.EncodeToString(sum[:])
}

func TestNewRefusesAnIncompleteConfig(t *testing.T) {
	for what, cfg := range map[string]servicesend.Config{
		"no endpoint":    {Region: region, AccessKeyID: keyID, SecretAccessKey: secret},
		"a relative URL": {Endpoint: "localhost:9050", Region: region, AccessKeyID: keyID, SecretAccessKey: secret},
		"no region":      {Endpoint: "http://localhost:9050", AccessKeyID: keyID, SecretAccessKey: secret},
		"no access key":  {Endpoint: "http://localhost:9050", Region: region, SecretAccessKey: secret},
		"no secret":      {Endpoint: "http://localhost:9050", Region: region, AccessKeyID: keyID},
	} {
		if _, err := servicesend.New(cfg); err == nil {
			t.Errorf("%s: New succeeded", what)
		}
	}
}

// requireSigned fails unless r is a POST to Path signed for sqs with the
// test key.
func requireSigned(t *testing.T, r *http.Request) {
	t.Helper()
	auth := r.Header.Get("Authorization")
	if r.Method != http.MethodPost || r.URL.Path != servicesend.Path ||
		!strings.Contains(auth, "Credential="+keyID+"/") ||
		!strings.Contains(auth, "/"+region+"/sqs/aws4_request") ||
		r.Header.Get("X-Amz-Date") == "" {
		t.Errorf("an unsigned or misdirected call: %s %s %q", r.Method, r.URL.Path, auth)
	}
}

// TestTheRequestIsSignedAndTheBodySentAsItIs checks, against a local
// listener, that the call is a signed POST for sqs, that the request
// carries the fields and the body, its <, > and & unescaped, and that the
// answer's request ID and message ID come back.
func TestTheRequestIsSignedAndTheBodySentAsItIs(t *testing.T) {
	const body = `{"Records":[{"s3":{"object":{"key":"a<b>&c"}}}]}` + "\u2028\t\n"
	want := servicesend.Request{
		QueueArn: queueARN, ServicePrincipal: s3Service, SourceArn: bucketARN,
		SourceAccount: "000000000000", MessageBody: body,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireSigned(t, r)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(raw, []byte(`a<b>&c`)) {
			t.Errorf("the body was HTML-escaped: %s", raw)
		}
		var req servicesend.Request
		if err = json.Unmarshal(raw, &req); err != nil || req != want {
			t.Errorf("the endpoint got %s, %v", raw, err)
		}
		w.Header().Set(servicesend.RequestIDHeader, "req-1")
		_ = json.NewEncoder(w).Encode(servicesend.Response{
			MessageID: "m-1", MD5OfMessageBody: md5Hex(body),
		})
	}))
	defer srv.Close()
	res, err := client(t, srv.URL).SendMessage(t.Context(), queueARN, s3Service, body,
		servicesend.WithSourceArn(bucketARN), servicesend.WithSourceAccount("000000000000"))
	if err != nil || *res != (servicesend.Result{
		RequestID: "req-1", MessageID: "m-1", MD5OfMessageBody: md5Hex(body),
	}) {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

// TestWhatCannotBeSentAsItIsIsNotSent checks that a body that is not UTF-8,
// which JSON would carry as U+FFFD, and a body set by an option, are errors
// before any call.
func TestWhatCannotBeSentAsItIsIsNotSent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a call was made")
	}))
	defer srv.Close()
	c := client(t, srv.URL)
	if _, err := c.SendMessage(t.Context(), queueARN, s3Service, "a\xffb"); err == nil {
		t.Error("a body that is not UTF-8 was sent")
	}
	setBody := func(r *servicesend.Request) { r.MessageBody = "other" }
	if _, err := c.SendMessage(t.Context(), queueARN, s3Service, "x", setBody); err == nil {
		t.Error("an option's MessageBody was sent beside SendMessage's")
	}
}

// TestADigestOfAnotherBodyIsAnError checks that the client holds the
// answer's MD5OfMessageBody to the body it sent, as the SDKs check it.
func TestADigestOfAnotherBodyIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(servicesend.Response{
			MessageID: "m-1", MD5OfMessageBody: md5Hex("other"),
		})
	}))
	defer srv.Close()
	if _, err := client(t, srv.URL).SendMessage(t.Context(), queueARN, s3Service,
		"body"); err == nil {
		t.Fatal("a digest of another body was accepted")
	}
}

// TestTheRefusalsCanBeToldApart checks the client's error mapping against
// a local listener's answers.
func TestTheRefusalsCanBeToldApart(t *testing.T) {
	for what, c := range map[string]struct {
		want   error
		body   string
		code   string
		status int
	}{
		"no queue": {
			want: servicesend.ErrNotFound, status: 404,
			body: `{"Code":"ResourceNotFound","Message":"m"}`,
		},
		"denied": {
			want: servicesend.ErrAccessDenied, status: 403,
			body: `{"Code":"AccessDenied","Message":"m"}`,
		},
		"an invalid message": {
			want: servicesend.ErrInvalidMessage, status: 400,
			body: `{"Code":"InvalidMessage","Message":"m"}`,
		},
		"a sigv4 refusal": {
			status: 403, code: "UnknownAccessKey",
			body: `{"Code":"UnknownAccessKey","Message":"m"}`,
		},
		"an invalid request": {
			status: 400, code: servicesend.CodeInvalidRequest,
			body: `{"Code":"InvalidRequest","Message":"m"}`,
		},
		"a 404 of another code":   {status: 404, code: "Other", body: `{"Code":"Other"}`},
		"a body that is not JSON": {status: 502, body: `oops`},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(c.body))
		}))
		_, err := client(t, srv.URL).SendMessage(t.Context(), queueARN, s3Service, "x")
		srv.Close()
		if c.want != nil {
			if !errors.Is(err, c.want) {
				t.Errorf("%s: got %v, want %v", what, err, c.want)
			}
			continue
		}
		var e *servicesend.Error
		if !errors.As(err, &e) || e.Status != c.status || e.Code != c.code {
			t.Errorf("%s: got %v", what, err)
		}
	}
}
