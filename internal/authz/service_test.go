package authz

import (
	"crypto/tls"
	"errors"
	"net/http/httptest"
	"testing"

	"tule.dev/policy"
)

const (
	s3Service = "s3.amazonaws.com"
	bucketARN = "arn:aws:s3:::photos"
)

// servicePolicy parses a queue policy of the statements, a JSON list's
// members.
func servicePolicy(t *testing.T, statements string) *policy.Policy {
	t.Helper()
	pol, err := newAuthorizer().ResourcePolicy(
		`{"Version":"2012-10-17","Statement":[` + statements + `]}`)
	if err != nil {
		t.Fatalf("queue policy: %v", err)
	}
	return pol
}

// s3Grant is the queue policy statement the S3 User Guide gives for event
// notifications ("Granting permissions to publish event notification
// messages to a destination"): SourceArn matched with ArnLike against
// arn:aws:s3:*:*:<bucket>, and SourceAccount.
const s3Grant = `{"Effect":"Allow","Principal":{"Service":"s3.amazonaws.com"},
	"Action":["SQS:SendMessage"],"Resource":"` + queueARN + `",
	"Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:s3:*:*:photos"},
	"StringEquals":{"aws:SourceAccount":"111122223333"}}}`

// fromBucket is the context S3 supplies for the bucket photos of
// 111122223333.
func fromBucket() map[string][]string {
	return map[string][]string{
		"aws:SourceArn":     {bucketARN},
		"aws:SourceAccount": {"111122223333"},
	}
}

func authorizeService(t *testing.T, a *Authorizer, service string, c Check) error {
	t.Helper()
	r := httptest.NewRequest("POST", "http://queues.local/_internal/send-message", nil)
	return a.AuthorizeService(r, service, c)
}

func requireServiceDenied(t *testing.T, err error, service string, reason policy.Reason) {
	t.Helper()
	var d *Denied
	if !errors.As(err, &d) {
		t.Fatalf("error = %v, want *Denied", err)
	}
	if d.Principal != service || d.Action != "sqs:SendMessage" || d.Resource != queueARN ||
		d.Reason != reason {
		t.Fatalf("denied %+v, want %s on %s by %s, reason %v", d, "sqs:SendMessage",
			queueARN, service, reason)
	}
}

func TestTheS3GrantAllowsItsBucketWithAnEmptyRegionAndAccount(t *testing.T) {
	a := newAuthorizer()
	c := Check{
		Action: "sqs:SendMessage", Resource: queueARN, Policy: servicePolicy(t, s3Grant),
		Context: fromBucket(),
	}
	// arn:aws:s3:*:*:photos matches arn:aws:s3:::photos: a * segment
	// matches an empty one.
	if err := authorizeService(t, a, s3Service, c); err != nil {
		t.Fatalf("the bucket: %v", err)
	}

	for what, ctx := range map[string]map[string][]string{
		"another bucket": {
			"aws:SourceArn": {"arn:aws:s3:::other"}, "aws:SourceAccount": {"111122223333"},
		},
		"another account":   {"aws:SourceArn": {bucketARN}, "aws:SourceAccount": {"444455556666"}},
		"no SourceArn":      {"aws:SourceAccount": {"111122223333"}},
		"no SourceAccount":  {"aws:SourceArn": {bucketARN}},
		"no source at all":  nil,
		"a prefixed bucket": {"aws:SourceArn": {bucketARN + "-x"}, "aws:SourceAccount": {"111122223333"}},
	} {
		denied := c
		denied.Context = ctx
		err := authorizeService(t, a, s3Service, denied)
		var d *Denied
		if !errors.As(err, &d) || d.Reason != policy.NoResourceAllow {
			t.Errorf("%s: %v, want a NoResourceAllow denial", what, err)
		}
	}
	requireServiceDenied(t, authorizeService(t, a, "sns.amazonaws.com", c),
		"sns.amazonaws.com", policy.NoResourceAllow)
	// The grant is for sqs:SendMessage alone.
	other := c
	other.Action = "sqs:DeleteQueue"
	var d *Denied
	if err := authorizeService(t, a, s3Service, other); !errors.As(err, &d) {
		t.Fatalf("another action: %v, want a denial", err)
	}
}

func TestAQueueWithoutAPolicyDeniesEveryService(t *testing.T) {
	err := authorizeService(t, newAuthorizer(), s3Service, Check{
		Action: "sqs:SendMessage", Resource: queueARN, Context: fromBucket(),
	})
	requireServiceDenied(t, err, s3Service, policy.NoResourceAllow)
}

func TestAnAWSPrincipalGrantDoesNotAllowAService(t *testing.T) {
	pol := servicePolicy(t, `{"Effect":"Allow","Principal":{"AWS":"111122223333"},
		"Action":"sqs:SendMessage","Resource":"`+queueARN+`"}`)
	err := authorizeService(t, newAuthorizer(), s3Service, Check{
		Action: "sqs:SendMessage", Resource: queueARN, Policy: pol, Context: fromBucket(),
	})
	requireServiceDenied(t, err, s3Service, policy.NoResourceAllow)
}

func TestAnyPrincipalWithAConditionAllowsTheService(t *testing.T) {
	pol := servicePolicy(t, `{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage",
		"Resource":"`+queueARN+`","Condition":{"ArnEquals":{"aws:SourceArn":"`+bucketARN+`"}}}`)
	a := newAuthorizer()
	c := Check{Action: "sqs:SendMessage", Resource: queueARN, Policy: pol, Context: fromBucket()}
	if err := authorizeService(t, a, s3Service, c); err != nil {
		t.Fatalf("the bucket: %v", err)
	}
	c.Context = nil
	requireServiceDenied(t, authorizeService(t, a, s3Service, c), s3Service,
		policy.NoResourceAllow)
}

func TestADenyBeatsTheGrant(t *testing.T) {
	pol := servicePolicy(t, s3Grant+`,{"Effect":"Deny","Principal":{"Service":"s3.amazonaws.com"},
		"Action":"sqs:SendMessage","Resource":"`+queueARN+`"}`)
	err := authorizeService(t, newAuthorizer(), s3Service, Check{
		Action: "sqs:SendMessage", Resource: queueARN, Policy: pol, Context: fromBucket(),
	})
	requireServiceDenied(t, err, s3Service, policy.ResourceDeny)
}

func TestANotPrincipalDenyExemptsTheServiceItNames(t *testing.T) {
	deny := `{"Effect":"Deny","NotPrincipal":{"Service":"s3.amazonaws.com"},
		"Action":"sqs:SendMessage","Resource":"` + queueARN + `"}`
	pol := servicePolicy(t, s3Grant+","+deny)
	a := newAuthorizer()
	c := Check{Action: "sqs:SendMessage", Resource: queueARN, Policy: pol, Context: fromBucket()}
	if err := authorizeService(t, a, s3Service, c); err != nil {
		t.Fatalf("the exempt service: %v", err)
	}
	requireServiceDenied(t, authorizeService(t, a, "sns.amazonaws.com", c),
		"sns.amazonaws.com", policy.ResourceDeny)
}

func TestAResourceTagConditionReadsTheQueuesTags(t *testing.T) {
	pol := servicePolicy(t, `{"Effect":"Allow","Principal":{"Service":"s3.amazonaws.com"},
		"Action":"sqs:SendMessage","Resource":"`+queueARN+`",
		"Condition":{"StringEquals":{"aws:ResourceTag/events":"on"}}}`)
	a := newAuthorizer()
	c := Check{
		Action: "sqs:SendMessage", Resource: queueARN, Policy: pol,
		Context: map[string][]string{"aws:ResourceTag/events": {"on"}},
	}
	if err := authorizeService(t, a, s3Service, c); err != nil {
		t.Fatalf("a matching tag: %v", err)
	}
	c.Context = map[string][]string{"aws:ResourceTag/events": {"off"}}
	requireServiceDenied(t, authorizeService(t, a, s3Service, c), s3Service,
		policy.NoResourceAllow)
}

// TestTheServiceContextKeys checks the global keys AuthorizeService
// supplies, and those tule.dev/policy sets for the service.
func TestTheServiceContextKeys(t *testing.T) {
	a := newAuthorizer()
	for key, value := range map[string]string{
		"aws:RequestedRegion":       "us-east-1",
		"aws:CurrentTime":           "2026-11-13T10:30:00Z",
		"aws:EpochTime":             "1794565800",
		"aws:SecureTransport":       "false",
		"aws:PrincipalServiceName":  s3Service,
		"aws:PrincipalIsAWSService": "true",
	} {
		pol := servicePolicy(t, `{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage",
			"Resource":"`+queueARN+`","Condition":{"StringEquals":{"`+key+`":"`+value+`"}}}`)
		if err := authorizeService(t, a, s3Service, Check{
			Action: "sqs:SendMessage", Resource: queueARN, Policy: pol,
		}); err != nil {
			t.Errorf("%s = %s: %v", key, value, err)
		}
	}

	// No aws:SourceIp: the address is the Tule service's.
	pol := servicePolicy(t, `{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage",
		"Resource":"`+queueARN+`","Condition":{"Null":{"aws:SourceIp":"true"}}}`)
	c := Check{Action: "sqs:SendMessage", Resource: queueARN, Policy: pol}
	if err := authorizeService(t, a, s3Service, c); err != nil {
		t.Fatalf("aws:SourceIp absent: %v", err)
	}

	// Over TLS, aws:SecureTransport is true.
	pol = servicePolicy(t, `{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage",
		"Resource":"`+queueARN+`","Condition":{"Bool":{"aws:SecureTransport":"true"}}}`)
	r := httptest.NewRequest("POST", "https://queues.local/_internal/send-message", nil)
	r.TLS = &tls.ConnectionState{}
	if err := a.AuthorizeService(r, s3Service, Check{
		Action: "sqs:SendMessage", Resource: queueARN, Policy: pol,
	}); err != nil {
		t.Fatalf("over TLS: %v", err)
	}
}
