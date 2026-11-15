package authz

import (
	"testing"

	"tule.dev/policy"
)

// These tests pin what tule.dev/policy v0.4.0's Authorize supplies for
// every evaluated principal: aws:PrincipalIsAWSService is false, and
// aws:PrincipalServiceName is absent, whatever a check's context gives
// them.

func TestPrincipalIsAWSServiceIsFalseForAnEvaluatedPrincipal(t *testing.T) {
	a := newAuthorizer()
	send := Check{Action: "sqs:SendMessage", Resource: queueARN}
	for what, arn := range map[string]string{"a user": userARN, "a session": sessionARN} {
		if err := authorize(t, a, principal(arn,
			condition("Bool", "aws:PrincipalIsAWSService", "false")), send); err != nil {
			t.Errorf("%s: an Allow on false: %v", what, err)
		}
		requireDenied(t, authorize(t, a, principal(arn,
			condition("Bool", "aws:PrincipalIsAWSService", "true")), send),
			"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
	}

	// A Deny on false applies, where it lapsed while the key was absent.
	denyHuman := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"sqs:*",
		"Resource":"*","Condition":{"Bool":{"aws:PrincipalIsAWSService":"false"}}}]}`
	requireDenied(t, authorize(t, a, principal(userARN, allow("sqs:*", "*"), denyHuman), send),
		"sqs:SendMessage", queueARN, policy.IdentityDeny)

	// A check's context cannot make the principal a service.
	forged := send
	forged.Context = map[string][]string{"aws:PrincipalIsAWSService": {"true"}}
	requireDenied(t, authorize(t, a, principal(userARN,
		condition("Bool", "aws:PrincipalIsAWSService", "true")), forged),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
}

func TestPrincipalIsAWSServiceIsFalseInTheQueuePolicy(t *testing.T) {
	a := newAuthorizer()
	pol, err := a.ResourcePolicy(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny",
		"Principal":"*","Action":"sqs:SendMessage","Resource":"` + queueARN + `",
		"Condition":{"Bool":{"aws:PrincipalIsAWSService":"false"}}}]}`)
	if err != nil {
		t.Fatalf("queue policy: %v", err)
	}
	send := Check{Action: "sqs:SendMessage", Resource: queueARN, Policy: pol}
	requireDenied(t, authorize(t, a, principal(userARN, allow("sqs:SendMessage", "*")), send),
		"sqs:SendMessage", queueARN, policy.ResourceDeny)
}

func TestPrincipalServiceNameIsAbsentForAnEvaluatedPrincipal(t *testing.T) {
	a := newAuthorizer()
	send := Check{Action: "sqs:SendMessage", Resource: queueARN}
	if err := authorize(t, a, principal(userARN,
		condition("Null", "aws:PrincipalServiceName", "true")), send); err != nil {
		t.Fatalf("an Allow on the key being absent: %v", err)
	}
	// A check's context cannot supply it.
	forged := send
	forged.Context = map[string][]string{"aws:PrincipalServiceName": {"sqs.amazonaws.com"}}
	requireDenied(t, authorize(t, a, principal(userARN,
		condition("StringEquals", "aws:PrincipalServiceName", "sqs.amazonaws.com")), forged),
		"sqs:SendMessage", queueARN, policy.NoIdentityAllow)
	if err := authorize(t, a, principal(userARN,
		condition("Null", "aws:PrincipalServiceName", "true")), forged); err != nil {
		t.Fatalf("an Allow on the key being absent, with a forged value: %v", err)
	}
}
