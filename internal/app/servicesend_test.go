package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"tule.dev/identity/keysource"
	"tule.dev/sigv4"

	"tule.dev/queues/servicesend"
)

// serviceSendCall posts body to the service-send endpoint of url, signed
// with the key for service unless keyID is empty, and returns the status
// and the error document.
func serviceSendCall(
	t *testing.T, url, keyID, secret, service string, body []byte,
) (int, servicesend.ErrorResponse) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		url+servicesend.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if keyID != "" {
		sum := sha256.Sum256(body)
		if err = v4.NewSigner().SignHTTP(t.Context(),
			aws.Credentials{AccessKeyID: keyID, SecretAccessKey: secret}, req,
			hex.EncodeToString(sum[:]), service, "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var e servicesend.ErrorResponse
	if err = json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatalf("decode the answer of %d: %v", resp.StatusCode, err)
	}
	return resp.StatusCode, e
}

var missingQueueSend = []byte(`{"QueueArn":"arn:aws:sqs:us-east-1:000000000000:missing",` +
	`"ServicePrincipal":"s3.amazonaws.com","MessageBody":"x"}`)

// TestTheServiceSendEndpointTakesTheRootKeyAlone checks the endpoint's
// verification: an unsigned call, one signed for another service, and one
// signed with an IAM user's key, even a user allowed everything, are
// refused with the sigv4 Kind as the code, as identity's internal endpoints
// refuse them; the root key gets through to the endpoint, and a body over
// the verifier's bound is 413.
func TestTheServiceSendEndpointTakesTheRootKeyAlone(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("*", "*")))
	_, url := startApp(t, t.TempDir(), nil)

	for what, c := range map[string]struct {
		keyID, secret, service, code string
	}{
		"unsigned":                      {"", "", "", "MissingAuth"},
		"signed for another service":    {"test", "test", "lambda", "WrongService"},
		"signed with an IAM user's key": {"AKIDOTHER", "test", "sqs", "UnknownAccessKey"},
	} {
		status, e := serviceSendCall(t, url, c.keyID, c.secret, c.service, missingQueueSend)
		if status != http.StatusForbidden || e.Code != c.code || e.Message == "" {
			t.Errorf("%s: got %d %+v, want 403 %s", what, status, e, c.code)
		}
	}

	status, e := serviceSendCall(t, url, "test", "test", "sqs", missingQueueSend)
	if status != http.StatusNotFound || e.Code != servicesend.CodeResourceNotFound {
		t.Fatalf("the root key: got %d %+v, want 404 %s", status, e,
			servicesend.CodeResourceNotFound)
	}

	status, e = serviceSendCall(t, url, "test", "test", "sqs",
		bytes.Repeat([]byte(" "), maxBodySize+1))
	if status != http.StatusRequestEntityTooLarge || e.Code != servicesend.CodeRequestTooLarge {
		t.Fatalf("an oversized body: got %d %+v, want 413 %s", status, e,
			servicesend.CodeRequestTooLarge)
	}
}

// rootIdentity is an identity whose signing-key endpoint answers the key
// AKIDOTHER, secret "test", with the principal p.
func rootIdentity(t *testing.T, p keysource.Principal) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req keysource.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			req.AccessKeyID != "AKIDOTHER" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(keysource.ErrorResponse{
				Code: keysource.CodeUnknownAccessKey,
			})
			return
		}
		p.AccessKeyID = req.AccessKeyID
		_ = json.NewEncoder(w).Encode(keysource.Response{
			SigningKey: sigv4.DeriveSigningKey("test", req.Date, req.Region, req.Service),
			Principal:  p,
			Version:    keysource.Version,
		})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestTheServiceSendEndpointTakesIdentitysRootOfItsAccount checks, through
// the app, that a key identity issued the root user of the service's
// account is accepted, and one of another account's root user refused as
// unknown.
func TestTheServiceSendEndpointTakesIdentitysRootOfItsAccount(t *testing.T) {
	root := func(account string) keysource.Principal {
		return keysource.Principal{
			AccountID: account, ARN: "arn:aws:iam::" + account + ":root",
			PrincipalARN: "arn:aws:iam::" + account + ":root", UserID: account,
			PrincipalType: string(sigv4.PrincipalTypeAccount), Root: true,
		}
	}
	for what, c := range map[string]struct {
		code    string
		account string
		status  int
	}{
		"the service's account": {servicesend.CodeResourceNotFound, "000000000000", 404},
		"another account":       {"UnknownAccessKey", "111111111111", 403},
	} {
		t.Run(what, func(t *testing.T) {
			t.Setenv("TULE_Auth_IdentityEndpoint", rootIdentity(t, root(c.account)))
			_, url := startApp(t, t.TempDir(), nil)
			status, e := serviceSendCall(t, url, "AKIDOTHER", "test", "sqs", missingQueueSend)
			if status != c.status || e.Code != c.code {
				t.Fatalf("got %d %+v, want %d %s", status, e, c.status, c.code)
			}
		})
	}
}

// fixedKeys is a key source that answers every scope with one principal.
type fixedKeys struct {
	p sigv4.Principal
}

func (f fixedKeys) SigningKey(context.Context, sigv4.Scope) (sigv4.Key, error) {
	return sigv4.Key{Signing: []byte("k"), Principal: f.p}, nil
}

func TestRootOnlyPassesTheServicesOwnRootAlone(t *testing.T) {
	root := sigv4.Principal{Root: true, Type: sigv4.PrincipalTypeAccount, AccountID: "000000000000"}
	other := root
	other.AccountID = "111111111111"
	user := sigv4.Principal{Type: sigv4.PrincipalTypeUser, AccountID: "000000000000"}
	rootUser := user
	rootUser.Root = true
	for what, c := range map[string]struct {
		want error
		p    sigv4.Principal
	}{
		"the service's root":     {p: root},
		"another account's root": {p: other, want: sigv4.ErrUnknownAccessKey},
		"a user":                 {p: user, want: sigv4.ErrUnknownAccessKey},
		"a user marked Root":     {p: rootUser, want: sigv4.ErrUnknownAccessKey},
	} {
		_, err := rootOnly{keys: fixedKeys{c.p}, account: "000000000000"}.
			SigningKey(t.Context(), sigv4.Scope{})
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", what, err, c.want)
		}
	}
}

// signedSQSPost posts body to url, the path as written, signed with keyID
// and the secret "test" for sqs, and returns the status and the answer.
func signedSQSPost(t *testing.T, url, keyID string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url,
		bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if err = v4.NewSigner().SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: keyID, SecretAccessKey: "test"}, req,
		hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(got)
}

// TestAnEncodedServiceSendPathIsNotTheEndpoint checks that a path that
// decodes to the endpoint's but is sent encoded, which the router does not
// route to the endpoint, is verified and served as any other request: an
// IAM user's key, which the endpoint refuses as unknown, is evaluated as on
// the SQS API, the root key's call sends nothing, and neither is answered
// in the endpoint's JSON form.
func TestAnEncodedServiceSendPathIsNotTheEndpoint(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("*", "*")))
	c, url := startApp(t, t.TempDir(), nil)
	arn := "arn:aws:sqs:us-east-1:000000000000:events"
	u := createQueue(t, c, "events", map[string]string{"Policy": `{"Version":"2012-10-17",` +
		`"Statement":[{"Effect":"Allow","Principal":{"Service":"s3.amazonaws.com"},` +
		`"Action":"sqs:SendMessage","Resource":"` + arn + `"}]}`})
	body := []byte(`{"QueueArn":"` + arn + `","ServicePrincipal":"s3.amazonaws.com",` +
		`"MessageBody":"x"}`)

	// The endpoint itself sends, and refuses the user's key in its form.
	if status, got := signedSQSPost(
		t,
		url+servicesend.Path,
		"test",
		body,
	); status != http.StatusOK {
		t.Fatalf("the endpoint: %d %s", status, got)
	}
	if status, got := signedSQSPost(t, url+servicesend.Path, "AKIDOTHER", body); status !=
		http.StatusForbidden || !strings.Contains(got, `"UnknownAccessKey"`) {
		t.Fatalf("the endpoint with a user's key: %d %s", status, got)
	}
	for _, path := range []string{"/_internal/send%2Dmessage", "/%5Finternal/send-message"} {
		for _, key := range []string{"test", "AKIDOTHER"} {
			status, got := signedSQSPost(t, url+path, key, body)
			if status == http.StatusOK || strings.Contains(got, `"Code"`) {
				t.Errorf("%s with %s: answered %d %s, as the endpoint", path, key, status, got)
			}
		}
	}
	out, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(u),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := out.Attributes["ApproximateNumberOfMessages"]; n != "1" {
		t.Fatalf("messages in the queue: %s, want the endpoint's 1", n)
	}
}
