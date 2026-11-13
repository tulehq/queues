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

	"tule.dev/policy"
	"tule.dev/sigv4"
)

const (
	userARN    = "arn:aws:iam::111122223333:user/team/dev"
	sessionARN = "arn:aws:sts::111122223333:assumed-role/worker/job-1"
	roleARN    = "arn:aws:iam::111122223333:role/jobs/worker"
	queueARN   = "arn:aws:sqs:us-east-1:111122223333:orders"
	rootARN    = "arn:aws:iam::111122223333:root"
)

var fixedNow = time.Date(2026, 11, 13, 10, 30, 0, 0, time.UTC)

func newAuthorizer() *Authorizer {
	return New("111122223333", "us-east-1", func() time.Time { return fixedNow })
}

// principal is the principal of arn, an IAM user's, a role session's or
// the root user's ARN, with the attributes identity's key source hands
// over for it: a session's role is roleARN's when its account and name
// match, else the role of its name at path /.
func principal(arn string, docs ...string) sigv4.Principal {
	account := strings.Split(arn, ":")[4]
	p := sigv4.Principal{AccountID: account, ARN: arn, AccessKeyID: "AKIAEXAMPLE"}
	switch {
	case strings.HasSuffix(arn, ":root"):
		p.Type, p.PrincipalARN, p.UserID = sigv4.PrincipalTypeAccount, arn, account
		p.Root = true
	case strings.Contains(arn, ":assumed-role/"):
		parts := strings.Split(arn, "/")
		p.Type = sigv4.PrincipalTypeAssumedRole
		p.UserID = "AROAEXAMPLEROLEID01:" + parts[2]
		p.PrincipalARN = "arn:aws:iam::" + account + ":role/" + parts[1]
		if account == "111122223333" && parts[1] == "worker" {
			p.PrincipalARN = roleARN
		}
	default:
		p.Type, p.PrincipalARN, p.UserID = sigv4.PrincipalTypeUser, arn, "AIDAEXAMPLEUSERID01"
	}
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

func requireDenied(t *testing.T, err error, action, resource string, reason policy.Reason) {
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
	p := principal(rootARN, deny("*", "*"))
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
	requireDenied(t, err, "sqs:SendMessage", queueARN, policy.NoIdentityAllow)
}

func TestAnAllowAllowsOnlyItsActionAndResource(t *testing.T) {
	a := newAuthorizer()
	p := principal(userARN, allow("sqs:SendMessage", queueARN))
	if err := authorize(t, a, p, Check{Action: "sqs:SendMessage", Resource: queueARN}); err != nil {
		t.Fatalf("allowed check: %v", err)
	}
	err := authorize(t, a, p, Check{Action: "sqs:ReceiveMessage", Resource: queueARN})
	requireDenied(t, err, "sqs:ReceiveMessage", queueARN, policy.NoIdentityAllow)
	other := "arn:aws:sqs:us-east-1:111122223333:other"
	err = authorize(t, a, p, Check{Action: "sqs:SendMessage", Resource: other})
	requireDenied(t, err, "sqs:SendMessage", other, policy.NoIdentityAllow)
}

func TestAnExplicitDenyBeatsAnAllow(t *testing.T) {
	p := principal(userARN, allow("sqs:*", "*"), deny("sqs:DeleteQueue", queueARN))
	err := authorize(t, newAuthorizer(), p, Check{Action: "sqs:DeleteQueue", Resource: queueARN})
	requireDenied(t, err, "sqs:DeleteQueue", queueARN, policy.IdentityDeny)
}

func TestEveryCheckMustBeAllowed(t *testing.T) {
	p := principal(userARN, allow("sqs:CreateQueue", "*"))
	err := authorize(t, newAuthorizer(), p,
		Check{Action: "sqs:CreateQueue", Resource: queueARN},
		Check{Action: "sqs:TagQueue", Resource: queueARN})
	requireDenied(t, err, "sqs:TagQueue", queueARN, policy.NoIdentityAllow)
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
	requireDenied(t, authorize(t, a, p, red), "sqs:TagQueue", queueARN, policy.NoIdentityAllow)
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

	p := principal(userARN)
	p.Tags = map[string]string{"Team": "blue"}
	ctx := a.Context(r, p)
	want := map[string]string{
		"aws:SourceIp":          "203.0.113.7",
		"aws:CurrentTime":       "2026-11-13T10:30:00Z",
		"aws:EpochTime":         strconv.FormatInt(fixedNow.Unix(), 10),
		"aws:SecureTransport":   "true",
		"aws:RequestedRegion":   "us-east-1",
		"aws:PrincipalTag/Team": "blue",
	}
	for k, v := range want {
		if got := ctx[k]; !slices.Equal(got, []string{v}) {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	// The principal's own keys are Authorize's to supply, from the Caller.
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

func TestTheCallerIsThePrincipalsAttributes(t *testing.T) {
	p := principal(sessionARN)
	want := policy.Caller{
		AccountID: "111122223333", ARN: sessionARN, PrincipalARN: roleARN,
		UserID: "AROAEXAMPLEROLEID01:job-1", Type: policy.PrincipalTypeAssumedRole,
	}
	if got := Caller(p); got != want {
		t.Fatalf("Caller = %+v, want %+v", got, want)
	}
}

// condition is an identity policy allowing sqs:SendMessage on every queue
// when the condition holds.
func condition(op, key, value string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:SendMessage",
		"Resource":"*","Condition":{"` + op + `":{"` + key + `":"` + value + `"}}}]}`
}

func TestARoleSessionsPrincipalArnHasItsRolesPath(t *testing.T) {
	a := newAuthorizer()
	send := Check{Action: "sqs:SendMessage", Resource: queueARN}
	if err := authorize(t, a, principal(sessionARN,
		condition("ArnLike", "aws:PrincipalArn", "arn:aws:iam::*:role/jobs/*")), send); err != nil {
		t.Fatalf("a condition on the role's path: %v", err)
	}
	requireDenied(t, authorize(t, a, principal(sessionARN,
		condition("ArnEquals", "aws:PrincipalArn", "arn:aws:iam::111122223333:role/worker")), send),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
}

func TestTheUsersKeysComeFromTheCaller(t *testing.T) {
	a := newAuthorizer()
	send := Check{Action: "sqs:SendMessage", Resource: queueARN}
	for key, value := range map[string]string{
		"aws:userid":           "AIDAEXAMPLEUSERID01",
		"aws:PrincipalType":    "User",
		"aws:PrincipalAccount": "111122223333",
		"aws:username":         "dev",
		"aws:PrincipalArn":     userARN,
	} {
		if err := authorize(t, a, principal(userARN, condition("StringEquals", key, value)),
			send); err != nil {
			t.Errorf("%s = %s: %v", key, value, err)
		}
	}
	// A request's own context cannot set them.
	forged := send
	forged.Context = map[string][]string{"aws:username": {"admin"}}
	requireDenied(t, authorize(t, a, principal(userARN,
		condition("StringEquals", "aws:username", "admin")), forged),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
}

func TestAPrincipalTagIsAConditionKey(t *testing.T) {
	a := newAuthorizer()
	send := Check{Action: "sqs:SendMessage", Resource: queueARN}
	p := principal(userARN, condition("StringEquals", "aws:PrincipalTag/team", "blue"))
	p.Tags = map[string]string{"Team": "blue"}
	if err := authorize(t, a, p, send); err != nil {
		t.Fatalf("a matching tag: %v", err)
	}
	p.Tags = map[string]string{"Team": "red"}
	requireDenied(t, authorize(t, a, p, send), "sqs:SendMessage", queueARN,
		policy.NoIdentityAllow)

	// Keys differing only by case would feed one key: identity never hands
	// them over, and they are not evaluated.
	p.Tags = map[string]string{"Team": "blue", "team": "red"}
	err := authorize(t, a, p, send)
	var d *Denied
	if !errors.Is(err, errPrincipal) || errors.As(err, &d) {
		t.Fatalf("error = %v, want errPrincipal", err)
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
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
}

func TestTheMessageForm(t *testing.T) {
	if got, want := Message(userARN, "sqs:sendmessage", queueARN, policy.NoIdentityAllow),
		"User: "+userARN+" is not authorized to perform: sqs:sendmessage on resource: "+
			queueARN+" because no identity-based policy allows the sqs:sendmessage action"; got != want {
		t.Errorf("implicit:\n got %s\nwant %s", got, want)
	}
	if got, want := Message(userARN, "sqs:sendmessage", queueARN, policy.IdentityDeny),
		"User: "+userARN+" is not authorized to perform: sqs:sendmessage on resource: "+
			queueARN+" with an explicit deny in an identity-based policy"; got != want {
		t.Errorf("explicit:\n got %s\nwant %s", got, want)
	}
}

func TestTheResourceBasedMessageForm(t *testing.T) {
	if got, want := Message(userARN, "sqs:sendmessage", queueARN, policy.NoResourceAllow),
		"User: "+userARN+" is not authorized to perform: sqs:sendmessage on resource: "+
			queueARN+" because no resource-based policy allows the sqs:sendmessage action"; got != want {
		t.Errorf("resource-based:\n got %s\nwant %s", got, want)
	}
}

const memberSessionARN = "arn:aws:sts::444455556666:assumed-role/OrganizationAccountAccessRole/e2e"

// memberPrincipal is a principal of another account than the authorizer's.
func memberPrincipal(docs ...string) sigv4.Principal {
	return principal(memberSessionARN, docs...)
}

// queuePolicy parses a queue policy whose one statement has effect for
// principal (a JSON value) on sqs:SendMessage of the queue.
func queuePolicy(t *testing.T, effect, principal string) *policy.Policy {
	t.Helper()
	pol, err := newAuthorizer().ResourcePolicy(`{"Version":"2012-10-17","Statement":[{"Effect":"` +
		effect + `","Principal":` + principal + `,"Action":"sqs:SendMessage","Resource":"` + queueARN + `"}]}`)
	if err != nil {
		t.Fatalf("queue policy: %v", err)
	}
	return pol
}

func TestAnotherAccountsPrincipalNeedsBothPolicies(t *testing.T) {
	a := newAuthorizer()
	check := Check{Action: "sqs:SendMessage", Resource: queueARN}
	granted := check
	granted.Policy = queuePolicy(t, "Allow", `{"AWS":"444455556666"}`)

	// Allowed by its identity policies, it still needs the queue policy.
	requireDenied(t, authorize(t, a, memberPrincipal(allow("*", "*")), check),
		"sqs:SendMessage", queueARN, policy.NoResourceAllow)
	if err := authorize(t, a, memberPrincipal(allow("*", "*")), granted); err != nil {
		t.Fatalf("both policies allow: %v", err)
	}
	// And the queue policy alone is not enough.
	requireDenied(t, authorize(t, a, memberPrincipal(), granted),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
	requireDenied(t, authorize(t, a, memberPrincipal(allow("*", "*"), deny("sqs:*", "*")), granted),
		"sqs:SendMessage", queueARN, policy.IdentityDeny)

	// Another account's root user is not root here; it has every
	// identity-based permission in its account, and needs the grant.
	root := principal("arn:aws:iam::444455556666:root")
	if isRoot, err := a.IsRoot(root); isRoot || err != nil {
		t.Fatalf("another account's root principal: IsRoot = %t, %v", isRoot, err)
	}
	requireDenied(t, authorize(t, a, root, check), "sqs:SendMessage", queueARN,
		policy.NoResourceAllow)
	if err := authorize(t, a, root, granted); err != nil {
		t.Fatalf("another account's root with the grant: %v", err)
	}
}

func TestAQueuePolicyDenyBeatsAnIdentityAllow(t *testing.T) {
	check := Check{
		Action: "sqs:SendMessage", Resource: queueARN,
		Policy: queuePolicy(t, "Deny", `"*"`),
	}
	requireDenied(t, authorize(t, newAuthorizer(), principal(userARN, allow("*", "*")), check),
		"sqs:SendMessage", queueARN, policy.ResourceDeny)
}

func TestWithinTheAccountAQueuePolicyGrantsAUserDirectly(t *testing.T) {
	a := newAuthorizer()
	direct := Check{
		Action: "sqs:SendMessage", Resource: queueARN,
		Policy: queuePolicy(t, "Allow", `{"AWS":"`+userARN+`"}`),
	}
	if err := authorize(t, a, principal(userARN), direct); err != nil {
		t.Fatalf("a grant naming the IAM user: %v", err)
	}
	// A grant to the account delegates to the account's own policies.
	account := direct
	account.Policy = queuePolicy(t, "Allow", `{"AWS":"`+rootARN+`"}`)
	requireDenied(t, authorize(t, a, principal(userARN), account),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
	if err := authorize(t, a, principal(userARN, allow("sqs:SendMessage", "*")),
		account); err != nil {
		t.Fatalf("an account grant with the identity allow: %v", err)
	}
}

func TestAGrantToARoleWithAPathReachesItsSessions(t *testing.T) {
	a := newAuthorizer()
	check := Check{
		Action: "sqs:SendMessage", Resource: queueARN,
		Policy: queuePolicy(t, "Allow", `{"AWS":"`+roleARN+`"}`),
	}
	if err := authorize(t, a, principal(sessionARN), check); err != nil {
		t.Fatalf("a session of the role: %v", err)
	}
	// The session of a deleted role is no session of the role.
	orphan := principal(sessionARN)
	orphan.PrincipalARN = ""
	requireDenied(t, authorize(t, a, orphan, check), "sqs:SendMessage", queueARN,
		policy.NoIdentityAllow)
}

func TestAStoredQueuePolicyThatCannotBeEvaluatedFailsClosed(t *testing.T) {
	a := newAuthorizer()
	if pol, err := a.ResourcePolicy(""); pol != nil || err != nil {
		t.Fatalf("no policy = %v, %v; want nil, nil", pol, err)
	}
	for name, doc := range map[string]string{
		"malformed":         `{"Statement":`,
		"without principal": allow("sqs:*", queueARN),
		"too large": `{"Statement":{"Effect":"Allow","Principal":"*","Action":"sqs:*",` +
			`"Resource":"arn:aws:sqs:::` + strings.Repeat("q", 8192) + `"}}`,
	} {
		if _, err := a.ResourcePolicy(doc); !errors.Is(err, errResourcePolicy) {
			t.Errorf("%s: error = %v, want errResourcePolicy", name, err)
		}
	}
}

func TestRootAndTypeOutOfStepAreAFault(t *testing.T) {
	a := newAuthorizer()
	check := Check{Action: "sqs:SendMessage", Resource: queueARN}
	accountNotRoot := principal("arn:aws:iam::444455556666:root", allow("*", "*"))
	accountNotRoot.Root = false
	// A user of the service's own account marked Root would otherwise skip
	// evaluation as the account's root user.
	rootUser := principal(userARN, deny("*", "*"))
	rootUser.Root = true
	for name, p := range map[string]sigv4.Principal{
		"Type Account without Root": accountNotRoot,
		"Root on a user":            rootUser,
	} {
		if isRoot, err := a.IsRoot(p); isRoot || !errors.Is(err, errPrincipal) {
			t.Errorf("%s: IsRoot = %t, %v; want errPrincipal", name, isRoot, err)
		}
		err := authorize(t, a, p, check)
		var d *Denied
		if !errors.Is(err, errPrincipal) || errors.As(err, &d) {
			t.Errorf("%s: error = %v, want errPrincipal", name, err)
		}
	}
}

func TestAnUnsharedActionIsNeverGrantedAcrossAccounts(t *testing.T) {
	a := newAuthorizer()
	allowAll := queuePolicy(t, "Allow", `"*"`)
	check := Check{
		Action: "sqs:SendMessage", Resource: queueARN, Policy: allowAll, Unshared: true,
	}
	// Both policies allow it, and it is still denied for another account.
	requireDenied(t, authorize(t, a, memberPrincipal(allow("*", "*")), check),
		"sqs:SendMessage", queueARN, policy.NoResourceAllow)
	// A Deny in the queue policy keeps its own message.
	denied := check
	denied.Policy = queuePolicy(t, "Deny", `"*"`)
	requireDenied(t, authorize(t, a, memberPrincipal(allow("*", "*")), denied),
		"sqs:SendMessage", queueARN, policy.ResourceDeny)
	// And a missing identity Allow keeps its.
	requireDenied(t, authorize(t, a, memberPrincipal(), check),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
	// Within the account the queue policy grants it as usual.
	if err := authorize(t, a, principal(userARN), check); err != nil {
		t.Fatalf("a principal of the account: %v", err)
	}
}
