package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"tule.dev/identity/principals"
)

const (
	devARN = "arn:aws:iam::000000000000:user/dev"
	devID  = "AIDAEXAMPLEUSERID01"
)

// principalIDs stands in for identity's principal-ID endpoint: it resolves
// the users it holds, and records the ARNs and IDs each call asks about.
type principalIDs struct {
	users  map[string]string // ARN to unique ID
	asked  [][]string
	status int
	mu     sync.Mutex
}

func (p *principalIDs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != principals.Path {
		http.NotFound(w, r)
		return
	}
	var req principals.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, append(slices.Clone(req.ARNs), req.IDs...))
	if p.status != 0 {
		w.WriteHeader(p.status)
		_, _ = w.Write([]byte(`{"Code":"InternalError","Message":"store down"}`))
		return
	}
	out := principals.Response{IDs: map[string]string{}, ARNs: map[string]string{}}
	for _, arn := range req.ARNs {
		if id, ok := p.users[arn]; ok {
			out.IDs[arn] = id
		}
	}
	for _, id := range req.IDs {
		for arn, uid := range p.users {
			if uid == id {
				out.ARNs[id] = arn
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// set replaces the users, and sets the status every call fails with, 0
// for none.
func (p *principalIDs) set(users map[string]string, status int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.users, p.status = users, status
}

// take returns what the calls since the last take asked about.
func (p *principalIDs) take() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.asked
	p.asked = nil
	return out
}

// startBindingApp starts the app in dir with an identity endpoint whose
// principal-ID endpoint holds dev.
func startBindingApp(t *testing.T, dir string) (*sqs.Client, string, *principalIDs) {
	t.Helper()
	ids := &principalIDs{users: map[string]string{devARN: devID}}
	srv := httptest.NewServer(ids)
	t.Cleanup(srv.Close)
	t.Setenv("TULE_Auth_IdentityEndpoint", srv.URL)
	c, base := startApp(t, dir, nil)
	return c, base, ids
}

// grantDoc is a queue policy granting principal sqs:SendMessage on the
// orders queue.
func grantDoc(principal string) string {
	return `{"Version":"2012-10-17","Statement":[{"Sid":"Grant","Effect":"Allow",` +
		`"Principal":{"AWS":"` + principal + `"},"Action":"sqs:SendMessage","Resource":"` +
		ordersARN + `"}]}`
}

func policyOf(t *testing.T, c *sqs.Client, u string) string {
	t.Helper()
	out, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(u), AttributeNames: []types.QueueAttributeName{"Policy"},
	})
	if err != nil {
		t.Fatalf("get Policy: %v", err)
	}
	return out.Attributes["Policy"]
}

// inspectedPolicy returns the orders queue's Policy as the inspection API
// reports it, signed with the root key.
func inspectedPolicy(t *testing.T, base string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/_queues/queues", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	sum := sha256.Sum256(nil)
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
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, string(raw)
	}
	var page struct {
		Items []struct {
			Attributes map[string]string `json:"attributes"`
			Name       string            `json:"name"`
		} `json:"items"`
	}
	if err = json.Unmarshal(raw, &page); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	for _, q := range page.Items {
		if q.Name == "orders" {
			return resp.StatusCode, q.Attributes["Policy"]
		}
	}
	t.Fatalf("no orders queue in %s", raw)
	return 0, ""
}

// requireAsked fails unless the calls since the last take asked about want,
// one call per entry.
func requireAsked(t *testing.T, ids *principalIDs, what string, want ...[]string) {
	t.Helper()
	got := ids.take()
	if len(got) != len(want) {
		t.Fatalf("%s: calls asked %v, want %v", what, got, want)
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("%s: calls asked %v, want %v", what, got, want)
		}
	}
}

// addPermission grants account 111122223333 SendMessage on u under label.
func addPermission(t *testing.T, c *sqs.Client, u, label string) error {
	t.Helper()
	_, err := c.AddPermission(t.Context(), &sqs.AddPermissionInput{
		QueueUrl: aws.String(u), Label: aws.String(label),
		AWSAccountIds: []string{"111122223333"}, Actions: []string{"SendMessage"},
	})
	return err //nolint:wrapcheck // the SDK's error, checked by the caller
}

func removePermission(t *testing.T, c *sqs.Client, u, label string) {
	t.Helper()
	if _, err := c.RemovePermission(t.Context(), &sqs.RemovePermissionInput{
		QueueUrl: aws.String(u), Label: aws.String(label),
	}); err != nil {
		t.Fatalf("RemovePermission %s: %v", label, err)
	}
}

func requirePolicy(t *testing.T, c *sqs.Client, u, want string) {
	t.Helper()
	if got := policyOf(t, c, u); got != want {
		t.Fatalf("Policy:\n got %s\nwant %s", got, want)
	}
}

func TestEveryPolicyReadShows(t *testing.T) {
	c, base, ids := startBindingApp(t, t.TempDir())

	// CreateQueue binds, GetQueueAttributes and the inspection API show.
	u := createQueue(t, c, "orders", map[string]string{"Policy": grantDoc(devARN)})
	requireAsked(t, ids, "CreateQueue", []string{devARN})
	requirePolicy(t, c, u, grantDoc(devARN))
	requireAsked(t, ids, "GetQueueAttributes", []string{devID})
	if status, got := inspectedPolicy(t, base); status != http.StatusOK ||
		got != grantDoc(devARN) {
		t.Fatalf("inspected Policy: %d %s", status, got)
	}
	requireAsked(t, ids, "inspection", []string{devID})

	// A read that does not ask for the Policy does not show it.
	if _, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(u), AttributeNames: []types.QueueAttributeName{"DelaySeconds"},
	}); err != nil {
		t.Fatalf("get DelaySeconds: %v", err)
	}
	requireAsked(t, ids, "GetQueueAttributes without Policy")

	// The IAM user deleted, and created again under its ARN: the policy names
	// the old one, and shows its ID.
	ids.set(map[string]string{devARN: "AIDAEXAMPLEUSERID02"}, 0)
	requirePolicy(t, c, u, grantDoc(devID))
}

func TestEveryPolicyWriteBinds(t *testing.T) {
	c, _, ids := startBindingApp(t, t.TempDir())
	u := createQueue(t, c, "orders", nil)

	again := strings.Replace(grantDoc(devARN), "Grant", "Again", 1)
	if _, err := c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(u), Attributes: map[string]string{"Policy": again},
	}); err != nil {
		t.Fatalf("set Policy: %v", err)
	}
	requireAsked(t, ids, "SetQueueAttributes", []string{devARN})

	// AddPermission and RemovePermission keep the stored statement bound:
	// it names the ID, which Bind keeps, so they ask nothing, and the read
	// after asks to show the ID.
	if err := addPermission(t, c, u, "partner"); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	requireAsked(t, ids, "AddPermission")
	if got := policyOf(t, c, u); !strings.Contains(got, `"`+devARN+`"`) ||
		!strings.Contains(got, "arn:aws:iam::111122223333:root") {
		t.Fatalf("Policy after AddPermission: %s", got)
	}
	requireAsked(t, ids, "GetQueueAttributes after AddPermission", []string{devID})
	removePermission(t, c, u, "partner")
	requireAsked(t, ids, "RemovePermission")
	_ = policyOf(t, c, u)
	requireAsked(t, ids, "GetQueueAttributes after RemovePermission", []string{devID})
}

// TestAPolicyStoredUnboundIsBoundWhenAPermissionChanges stores policies
// without identity, as an earlier release did, then edits them with
// AddPermission and RemovePermission against identity, which bind what
// they write.
func TestAPolicyStoredUnboundIsBoundWhenAPermissionChanges(t *testing.T) {
	dir := t.TempDir()
	doc := `{"Version":"2012-10-17","Statement":[` +
		`{"Sid":"Dev","Effect":"Allow","Principal":{"AWS":"` + devARN + `"},` +
		`"Action":"sqs:SendMessage","Resource":"` + ordersARN + `"},` +
		`{"Sid":"Other","Effect":"Allow","Principal":{"AWS":"` + devARN + `"},` +
		`"Action":"sqs:ReceiveMessage","Resource":"` + ordersARN + `"}]}`
	t.Run("standalone", func(t *testing.T) {
		c, _ := startApp(t, dir, nil)
		createQueue(t, c, "orders", map[string]string{"Policy": doc})
		createQueue(t, c, "returns", map[string]string{"Policy": doc})
		createQueue(t, c, "stale", map[string]string{"Policy": doc})
	})

	c, base, ids := startBindingApp(t, dir)
	// Read back as stored: nothing in it is an ID.
	if got := policyOf(t, c, base+"/000000000000/orders"); got != doc {
		t.Fatalf("unbound Policy:\n got %s\nwant %s", got, doc)
	}
	requireAsked(t, ids, "reading an unbound policy")

	if err := addPermission(t, c, base+"/000000000000/orders", "partner"); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	requireAsked(t, ids, "AddPermission", []string{devARN})
	_ = policyOf(t, c, base+"/000000000000/orders")
	requireAsked(t, ids, "reading the policy AddPermission wrote", []string{devID})

	removePermission(t, c, base+"/000000000000/returns", "Other")
	requireAsked(t, ids, "RemovePermission", []string{devARN})
	_ = policyOf(t, c, base+"/000000000000/returns")
	requireAsked(t, ids, "reading the policy RemovePermission wrote", []string{devID})

	// An unbound policy naming a user that no longer exists cannot be
	// bound, so a permission change on it is refused as an invalid Policy,
	// while a bound one keeps the gone user's ID and is changed.
	ids.set(map[string]string{}, 0)
	add := func(queue string) error {
		return addPermission(t, c, base+"/000000000000/"+queue, "more")
	}
	requireAPIError(t, add("stale"), "InvalidAttributeValue", http.StatusBadRequest)
	if got := policyOf(t, c, base+"/000000000000/stale"); got != doc {
		t.Fatalf("Policy after the refused AddPermission:\n got %s\nwant %s", got, doc)
	}
	if err := add("returns"); err != nil {
		t.Fatalf("AddPermission on a bound policy whose user is gone: %v", err)
	}
	if got := policyOf(t, c, base+"/000000000000/returns"); !strings.Contains(got, `"`+devID+`"`) {
		t.Fatalf("a bound policy whose user is gone shows: %s", got)
	}
}

func TestAPolicyNamingAMissingPrincipalIsRefused(t *testing.T) {
	c, _, ids := startBindingApp(t, t.TempDir())
	gone := "arn:aws:iam::000000000000:user/gone"

	_, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("orders"), Attributes: map[string]string{"Policy": grantDoc(gone)},
	})
	requireAPIError(t, err, "InvalidAttributeValue", http.StatusBadRequest)
	if !strings.Contains(err.Error(),
		"Invalid value for the parameter Policy. Reason: Invalid principal in policy: "+gone) {
		t.Fatalf("message: %v", err)
	}
	if _, err = c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{
		QueueName: aws.String("orders"),
	}); err == nil {
		t.Fatal("the refused CreateQueue created the queue")
	}

	u := createQueue(t, c, "orders", map[string]string{"Policy": grantDoc(devARN)})
	ids.take()
	_, err = c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(u), Attributes: map[string]string{"Policy": grantDoc(gone)},
	})
	requireAPIError(t, err, "InvalidAttributeValue", http.StatusBadRequest)
	if got := policyOf(t, c, u); got != grantDoc(devARN) {
		t.Fatalf("Policy after the refused set: %s", got)
	}
}

func TestIdentityUnreachableFailsPolicyWritesAndReads(t *testing.T) {
	c, base, ids := startBindingApp(t, t.TempDir())
	u := createQueue(t, c, "orders", map[string]string{"Policy": grantDoc(devARN)})
	plain := createQueue(t, c, "plain", nil)
	ids.set(map[string]string{devARN: devID}, http.StatusInternalServerError)

	_, err := c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(u), Attributes: map[string]string{"Policy": grantDoc(devARN)},
	})
	requireAPIError(t, err, "InternalFailure", http.StatusInternalServerError)
	_, err = c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("new"), Attributes: map[string]string{"Policy": grantDoc(devARN)},
	})
	requireAPIError(t, err, "InternalFailure", http.StatusInternalServerError)
	_, err = c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(u), AttributeNames: []types.QueueAttributeName{"All"},
	})
	requireAPIError(t, err, "InternalFailure", http.StatusInternalServerError)
	if status, _ := inspectedPolicy(t, base); status != http.StatusInternalServerError {
		t.Fatalf("inspection: status %d, want 500", status)
	}

	// A policy without an ID or ARN to resolve needs no identity, nor does
	// a write or read without a policy.
	if _, err = c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(plain), Attributes: map[string]string{
			"Policy": grantDoc("111122223333"), "DelaySeconds": "5",
		},
	}); err != nil {
		t.Fatalf("set an account grant: %v", err)
	}
	if got := policyOf(t, c, plain); got != grantDoc("111122223333") {
		t.Fatalf("account grant shown: %s", got)
	}
	if _, err = c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(u), AttributeNames: []types.QueueAttributeName{"DelaySeconds"},
	}); err != nil {
		t.Fatalf("a read without the Policy: %v", err)
	}
}
