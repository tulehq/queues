package queues

import (
	"context"
	"errors"
	"fmt"

	"tule.dev/identity/principals"

	"tule.dev/queues/internal/contracts"
)

// bindPolicy returns the queue policy doc, which passed CheckPolicy, as it
// is stored: with each IAM user and role ARN of its principals replaced by
// the principal's unique ID, as SQS stores it, so that a user or role
// deleted and created again under the same ARN is not granted. Standalone
// (no PolicyPrincipals) there are no unique IDs, and doc is stored as sent.
//
// An ARN no user or role has is refused as SQS refuses it,
// InvalidAttributeValue for the Policy: "Amazon SQS validates the resource
// policy and checks for the IAM entity. If the IAM entity doesn't exist,
// you will receive an error" ("Troubleshoot Amazon SQS API errors", SQS
// Developer Guide). Any other failure, identity unreachable among them, is
// an internal fault, which fails the write.
func bindPolicy(ctx context.Context, p contracts.PolicyPrincipals, doc string) (string, error) {
	if p == nil || doc == "" {
		return doc, nil
	}
	bound, err := p.Bind(ctx, doc)
	var invalid *principals.InvalidPrincipalError
	switch {
	case errors.As(err, &invalid):
		return "", &contracts.PolicyError{Reason: "Invalid principal in policy: " + invalid.ARN}
	case err != nil:
		return "", fmt.Errorf("bind the queue policy's principals: %w", err)
	}
	// Identity's unique IDs are 21 characters, shorter than any user or
	// role ARN, and binding keeps the principals' number, so a bound policy
	// passes what the policy as sent passed; one that does not is a fault
	// between the two services, and is not stored.
	if pe := contracts.CheckPolicy(bound); pe != nil {
		return "", fmt.Errorf("the bound queue policy does not pass: %w", pe)
	}
	return bound, nil
}

// bindAttributes binds the Policy of checked CreateQueue or
// SetQueueAttributes attributes, when they set one, in place.
func (m *Module) bindAttributes(ctx context.Context, attrs map[string]string) error {
	doc, ok := attrs[contracts.AttrPolicy]
	if !ok {
		return nil
	}
	bound, err := bindPolicy(ctx, m.principals, doc)
	if err != nil {
		return err
	}
	attrs[contracts.AttrPolicy] = bound
	return nil
}

// binder is bindPolicy over m's principals, for the statement AddPermission
// generates.
func (m *Module) binder() contracts.BindFunc {
	return func(ctx context.Context, doc string) (string, error) {
		return bindPolicy(ctx, m.principals, doc)
	}
}

// ShowPolicy replaces the Policy in attrs, when it has one, with the policy
// as SQS shows it: each unique ID that a user or role still has becomes its
// current ARN, and one that no user or role has stays the ID. Standalone
// (no PolicyPrincipals) the policy is shown as stored. A failure to ask
// identity fails the read.
func ShowPolicy(ctx context.Context, p contracts.PolicyPrincipals, attrs map[string]string) error {
	doc, ok := attrs[contracts.AttrPolicy]
	if p == nil || !ok || doc == "" {
		return nil
	}
	shown, err := p.Show(ctx, doc)
	if err != nil {
		return fmt.Errorf("show the queue policy's principals: %w", err)
	}
	attrs[contracts.AttrPolicy] = shown
	return nil
}
