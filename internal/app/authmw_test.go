package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// fakeIdentity stands in for identity's signing-key endpoint, answering
// every lookup with status and body, and counts the lookups.
func fakeIdentity(t *testing.T, status int, body string) (string, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &calls
}

// withOtherKey signs a call with another access key than the root key.
func withOtherKey(o *sqs.Options) {
	o.Credentials = credentials.NewStaticCredentialsProvider("AKIDOTHER", "test", "")
}

func TestStandaloneRefusesAKeyOtherThanTheRootKey(t *testing.T) {
	c, _ := startApp(t, t.TempDir(), nil)

	_, err := c.ListQueues(context.Background(), &sqs.ListQueuesInput{}, withOtherKey)
	requireAPIError(t, err, "InvalidClientTokenId", http.StatusForbidden)
}

func TestAKeyIdentityDoesNotKnowIsRefused(t *testing.T) {
	url, calls := fakeIdentity(t, http.StatusBadRequest,
		`{"Code":"UnknownAccessKey","Message":"unknown"}`)
	t.Setenv("TULE_Auth_IdentityEndpoint", url)
	c, _ := startApp(t, t.TempDir(), nil)

	_, err := c.ListQueues(context.Background(), &sqs.ListQueuesInput{}, withOtherKey)
	requireAPIError(t, err, "InvalidClientTokenId", http.StatusForbidden)
	if calls.Load() != 1 {
		t.Fatalf("identity lookups = %d, want 1", calls.Load())
	}
}

func TestAFailedLookupAtIdentityIsAServiceFault(t *testing.T) {
	url, calls := fakeIdentity(t, http.StatusInternalServerError,
		`{"Code":"InternalFailure","Message":"the store failed"}`)
	t.Setenv("TULE_Auth_IdentityEndpoint", url)
	c, _ := startApp(t, t.TempDir(), nil)

	_, err := c.ListQueues(context.Background(), &sqs.ListQueuesInput{}, withOtherKey)
	requireAPIError(t, err, "InternalFailure", http.StatusInternalServerError)

	// The root key is checked first, so it works without identity.
	before := calls.Load()
	if _, err = c.ListQueues(context.Background(), &sqs.ListQueuesInput{}); err != nil {
		t.Fatalf("list queues with the root key: %v", err)
	}
	if calls.Load() != before {
		t.Fatal("a root key request asked identity")
	}
}

func TestAHungIdentityFailsClosed(t *testing.T) {
	// The fake never answers until the test ends. Cleanups run last first,
	// so the handler is released before the server closes.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	saved := identityTimeout
	identityTimeout = 100 * time.Millisecond
	t.Cleanup(func() { identityTimeout = saved })

	t.Setenv("TULE_Auth_IdentityEndpoint", srv.URL)
	c, _ := startApp(t, t.TempDir(), nil)

	start := time.Now()
	_, err := c.ListQueues(context.Background(), &sqs.ListQueuesInput{}, withOtherKey)
	requireAPIError(t, err, "InternalFailure", http.StatusInternalServerError)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the lookup took %v, want about the 100ms timeout", elapsed)
	}
}

// requireAPIError fails unless err is an API error with the given code,
// answered with the given status.
func requireAPIError(t *testing.T, err error, code string, status int) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != code {
		t.Fatalf("error = %v, want %s", err, code)
	}
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != status {
		t.Fatalf("error = %v, want status %d", err, status)
	}
}
