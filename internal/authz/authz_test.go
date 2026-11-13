package authz

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"tule.dev/sigv4"
)

const (
	userARN    = "arn:aws:iam::111122223333:user/team/dev"
	sessionARN = "arn:aws:sts::111122223333:assumed-role/worker/job-1"
	queueARN   = "arn:aws:sqs:us-east-1:111122223333:orders"
)

var fixedNow = time.Date(2026, 11, 13, 10, 30, 0, 0, time.UTC)

func newAuthorizer() *Authorizer {
	return New("111122223333", "us-east-1", func() time.Time { return fixedNow })
}

func principal(arn string, docs ...string) sigv4.Principal {
	p := sigv4.Principal{AccountID: "111122223333", ARN: arn, AccessKeyID: "AKIAEXAMPLE"}
	for _, d := range docs {
		p.Policies = append(p.Policies, json.RawMessage(d))
	}
	return p
}

func allow(action, resource string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"` + action +
		`","Resource":"` + resource + `"}]}`
}

func deny(action, resource string) string {
	return strings.Replace(allow(action, resource), "Allow", "Deny", 1)
}

func authorize(t *testing.T, a *Authorizer, p sigv4.Principal, checks ...Check) error {
	t.Helper()
	r := httptest.NewRequest("POST", "http://queues.local/", nil)
	return a.Authorize(r, p, checks...)
}

func requireDenied(t *testing.T, err error, action, resource string, reason Reason) {
	t.Helper()
	var d *Denied
	if !errors.As(err, &d) {
		t.Fatalf("error = %v, want *Denied", err)
	}
	if d.Action != action || d.Resource != resource || d.Reason != reason {
		t.Fatalf("denied %+v, want %s on %s, reason %v", d, action, resource, reason)
	}
}

func TestARootPrincipalIsNeverEvaluated(t *testing.T) {
	p := principal("arn:aws:iam::111122223333:root", deny("*", "*"))
	p.Root = true
	if err := authorize(
		t,
		newAuthorizer(),
		p,
		Check{Action: "sqs:DeleteQueue", Resource: queueARN},
	); err != nil {
		t.Fatalf("root: %v", err)
	}
}

func TestAPrincipalWithoutPoliciesIsImplicitlyDenied(t *testing.T) {
	err := authorize(t, newAuthorizer(), principal(userARN),
		Check{Action: "sqs:SendMessage", Resource: queueARN})
	requireDenied(t, err, "sqs:SendMessage", queueARN, NoIdentityAllow)
}

func TestAnAllowAllowsOnlyItsActionAndResource(t *testing.T) {
	a := newAuthorizer()
	p := principal(userARN, allow("sqs:SendMessage", queueARN))
	if err := authorize(t, a, p, Check{Action: "sqs:SendMessage", Resource: queueARN}); err != nil {
		t.Fatalf("allowed check: %v", err)
	}
	err := authorize(t, a, p, Check{Action: "sqs:ReceiveMessage", Resource: queueARN})
	requireDenied(t, err, "sqs:ReceiveMessage", queueARN, NoIdentityAllow)
	other := "arn:aws:sqs:us-east-1:111122223333:other"
	err = authorize(t, a, p, Check{Action: "sqs:SendMessage", Resource: other})
	requireDenied(t, err, "sqs:SendMessage", other, NoIdentityAllow)
}

func TestAnExplicitDenyBeatsAnAllow(t *testing.T) {
	p := principal(userARN, allow("sqs:*", "*"), deny("sqs:DeleteQueue", queueARN))
	err := authorize(t, newAuthorizer(), p, Check{Action: "sqs:DeleteQueue", Resource: queueARN})
	requireDenied(t, err, "sqs:DeleteQueue", queueARN, ExplicitDeny)
}

func TestEveryCheckMustBeAllowed(t *testing.T) {
	p := principal(userARN, allow("sqs:CreateQueue", "*"))
	err := authorize(t, newAuthorizer(), p,
		Check{Action: "sqs:CreateQueue", Resource: queueARN},
		Check{Action: "sqs:TagQueue", Resource: queueARN})
	requireDenied(t, err, "sqs:TagQueue", queueARN, NoIdentityAllow)
}

func TestACheckCarriesItsOwnContext(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:TagQueue",
		"Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/team":"blue"}}}]}`
	a, p := newAuthorizer(), principal(userARN, doc)
	blue := Check{
		Action: "sqs:TagQueue", Resource: queueARN,
		Context: map[string][]string{"aws:RequestTag/team": {"blue"}},
	}
	if err := authorize(t, a, p, blue); err != nil {
		t.Fatalf("matching tag: %v", err)
	}
	red := blue
	red.Context = map[string][]string{"aws:RequestTag/team": {"red"}}
	requireDenied(t, authorize(t, a, p, red), "sqs:TagQueue", queueARN, NoIdentityAllow)
}

func TestAnUnusablePolicyIsAFaultNotADenial(t *testing.T) {
	a := newAuthorizer()
	for name, doc := range map[string]string{
		"malformed":        `{"Statement":`,
		"with a principal": `{"Statement":{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}}`,
		"too large":        allow("sqs:*", "arn:aws:sqs:::"+strings.Repeat("q", maxPolicySize)),
	} {
		err := authorize(
			t,
			a,
			principal(userARN, doc),
			Check{Action: "sqs:ListQueues", Resource: "*"},
		)
		var d *Denied
		if !errors.Is(err, errPolicy) || errors.As(err, &d) {
			t.Fatalf("%s: error = %v, want errPolicy", name, err)
		}
	}
}

func TestTheCacheIsBounded(t *testing.T) {
	a := newAuthorizer()
	for i := range maxCachedPolicies + 10 {
		doc := allow("sqs:SendMessage", queueARN+strconv.Itoa(i))
		if err := authorize(t, a, principal(userARN, doc),
			Check{Action: "sqs:ListQueues", Resource: "*"}); err == nil {
			t.Fatal("an unrelated Allow allowed ListQueues")
		}
	}
	if n := len(a.cache); n != maxCachedPolicies {
		t.Fatalf("cache holds %d documents, want %d", n, maxCachedPolicies)
	}
}

func TestTheGlobalContextKeys(t *testing.T) {
	a := newAuthorizer()
	r := httptest.NewRequest("POST", "https://queues.local/", nil)
	r.RemoteAddr = "203.0.113.7:4567"
	r.TLS = &tls.ConnectionState{}

	ctx := a.Context(r, principal(userARN))
	want := map[string]string{
		"aws:SourceIp":         "203.0.113.7",
		"aws:CurrentTime":      "2026-11-13T10:30:00Z",
		"aws:EpochTime":        strconv.FormatInt(fixedNow.Unix(), 10),
		"aws:SecureTransport":  "true",
		"aws:PrincipalAccount": "111122223333",
		"aws:PrincipalArn":     userARN,
		"aws:PrincipalType":    "User",
		"aws:username":         "dev",
		"aws:RequestedRegion":  "us-east-1",
	}
	for k, v := range want {
		if got := ctx[k]; !slices.Equal(got, []string{v}) {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if len(ctx) != len(want) {
		t.Errorf("context has %d keys, want %d: %v", len(ctx), len(want), ctx)
	}

	plain := httptest.NewRequest("POST", "http://queues.local/", nil)
	if got := a.Context(plain, principal(userARN))["aws:SecureTransport"]; !slices.Equal(
		got,
		[]string{"false"},
	) {
		t.Errorf("aws:SecureTransport over HTTP = %q, want false", got)
	}
}

func TestARoleSessionIsItsRoleWithoutAPath(t *testing.T) {
	ctx := newAuthorizer().Context(httptest.NewRequest("POST", "/", nil), principal(sessionARN))
	if got := ctx["aws:PrincipalArn"]; !slices.Equal(
		got,
		[]string{"arn:aws:iam::111122223333:role/worker"},
	) {
		t.Errorf("aws:PrincipalArn = %q", got)
	}
	if got := ctx["aws:PrincipalType"]; !slices.Equal(got, []string{"AssumedRole"}) {
		t.Errorf("aws:PrincipalType = %q", got)
	}
	if _, ok := ctx["aws:username"]; ok {
		t.Error("a role session has aws:username")
	}
}

func TestTheRootUsersKeys(t *testing.T) {
	ctx := newAuthorizer().Context(httptest.NewRequest("POST", "/", nil),
		principal("arn:aws:iam::111122223333:root"))
	if got := ctx["aws:userid"]; !slices.Equal(got, []string{"111122223333"}) {
		t.Errorf("aws:userid = %q", got)
	}
	if got := ctx["aws:PrincipalType"]; !slices.Equal(got, []string{"Account"}) {
		t.Errorf("aws:PrincipalType = %q", got)
	}
}

func TestAPolicyVariableReadsTheUserName(t *testing.T) {
	doc := allow("sqs:*", "arn:aws:sqs:us-east-1:111122223333:${aws:username}-*")
	a, p := newAuthorizer(), principal(userARN, doc)
	if err := authorize(t, a, p, Check{
		Action:   "sqs:SendMessage",
		Resource: "arn:aws:sqs:us-east-1:111122223333:dev-jobs",
	}); err != nil {
		t.Fatalf("own queue: %v", err)
	}
	requireDenied(t, authorize(t, a, p, Check{Action: "sqs:SendMessage", Resource: queueARN}),
		"sqs:SendMessage", queueARN, NoIdentityAllow)
}

func TestTheMessageForm(t *testing.T) {
	if got, want := Message(userARN, "sqs:sendmessage", queueARN, NoIdentityAllow),
		"User: "+userARN+" is not authorized to perform: sqs:sendmessage on resource: "+
			queueARN+" because no identity-based policy allows the sqs:sendmessage action"; got != want {
		t.Errorf("implicit:\n got %s\nwant %s", got, want)
	}
	if got, want := Message(userARN, "sqs:sendmessage", queueARN, ExplicitDeny),
		"User: "+userARN+" is not authorized to perform: sqs:sendmessage on resource: "+
			queueARN+" with an explicit deny in an identity-based policy"; got != want {
		t.Errorf("explicit:\n got %s\nwant %s", got, want)
	}
}

func TestTheResourceBasedMessageForm(t *testing.T) {
	if got, want := Message(userARN, "sqs:sendmessage", queueARN, NoResourceAllow),
		"User: "+userARN+" is not authorized to perform: sqs:sendmessage on resource: "+
			queueARN+" because no resource-based policy allows the sqs:sendmessage action"; got != want {
		t.Errorf("resource-based:\n got %s\nwant %s", got, want)
	}
}

// memberPrincipal is a principal of another account than the authorizer's.
func memberPrincipal(docs ...string) sigv4.Principal {
	p := principal("arn:aws:sts::444455556666:assumed-role/OrganizationAccountAccessRole/e2e",
		docs...)
	p.AccountID = "444455556666"
	return p
}

func TestAnotherAccountsPrincipalIsDenied(t *testing.T) {
	a := newAuthorizer()
	check := Check{Action: "sqs:SendMessage", Resource: queueARN}

	// Allowed by its identity policies, it still needs a resource-based
	// policy, which nothing here grants.
	requireDenied(t, authorize(t, a, memberPrincipal(allow("*", "*")), check),
		"sqs:SendMessage", queueARN, NoResourceAllow)
	// Its identity policies are evaluated first.
	requireDenied(t, authorize(t, a, memberPrincipal(), check),
		"sqs:SendMessage", queueARN, NoIdentityAllow)
	requireDenied(t, authorize(t, a, memberPrincipal(allow("*", "*"), deny("sqs:*", "*")), check),
		"sqs:SendMessage", queueARN, ExplicitDeny)

	// Another account's root user is evaluated like any principal of that
	// account, not let through as root.
	root := memberPrincipal(allow("*", "*"))
	root.Root = true
	if a.IsRoot(root) {
		t.Fatal("another account's root principal is root here")
	}
	requireDenied(t, authorize(t, a, root, check), "sqs:SendMessage", queueARN, NoResourceAllow)
}
