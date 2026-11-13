package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"tule.dev/policy"
)

// The quotas of a queue policy ("Amazon SQS policy quotas", SQS Developer
// Guide): 8,192 bytes, 20 statements, 50 principals, 10 conditions and 7
// actions per statement.
const (
	MaxPolicySize       = 8192
	MaxPolicyStatements = 20
	MaxPolicyPrincipals = 50
	MaxPolicyConditions = 10
	MaxStatementActions = 7
)

// PolicyError is a queue policy SQS refuses: malformed, or over one of its
// quotas (OverLimit).
type PolicyError struct {
	Reason    string
	OverLimit bool
}

func (e *PolicyError) Error() string {
	return "queues: invalid Policy: " + e.Reason
}

// CheckPolicy checks a queue policy: at most MaxPolicySize bytes, a
// resource-based policy tule.dev/policy's ParseResource accepts (each
// statement with a Principal or NotPrincipal and a Resource or
// NotResource), and within the statement, principal, condition and action
// quotas. It returns nil or a *PolicyError.
//
// The quotas are counted on the document ParseResource accepted, which has
// no repeated or unknown element: statements in Statement (one for a single
// object), each statement's Action or NotAction values, the values of
// every statement's Principal or NotPrincipal ("*" being one), and every
// statement's condition keys under each operator. SQS does not document
// how it counts principals and conditions, nor whether across the policy
// or per statement; across the policy is this service's choice.
func CheckPolicy(doc string) *PolicyError {
	if len(doc) > MaxPolicySize {
		return &PolicyError{OverLimit: true, Reason: fmt.Sprintf(
			"The policy is %d bytes, over the maximum of %d.", len(doc), MaxPolicySize)}
	}
	if _, err := policy.ParseResource([]byte(doc)); err != nil {
		var pe *policy.ParseError
		if errors.As(err, &pe) {
			return &PolicyError{Reason: pe.Message}
		}
		return &PolicyError{Reason: err.Error()}
	}
	stmts, err := policyStatements(doc)
	if err != nil {
		return &PolicyError{Reason: err.Error()}
	}
	if len(stmts) > MaxPolicyStatements {
		return overLimit("statements", len(stmts), MaxPolicyStatements)
	}
	principals, conditions := 0, 0
	for _, st := range stmts {
		actions := valueCount(st["Action"]) + valueCount(st["NotAction"])
		if actions > MaxStatementActions {
			return overLimit("actions in a statement", actions, MaxStatementActions)
		}
		principals += principalCount(st["Principal"]) + principalCount(st["NotPrincipal"])
		conditions += conditionCount(st["Condition"])
	}
	if principals > MaxPolicyPrincipals {
		return overLimit("principals", principals, MaxPolicyPrincipals)
	}
	if conditions > MaxPolicyConditions {
		return overLimit("conditions", conditions, MaxPolicyConditions)
	}
	return nil
}

func overLimit(what string, n, limit int) *PolicyError {
	return &PolicyError{OverLimit: true, Reason: fmt.Sprintf(
		"The policy has %d %s, over the maximum of %d.", n, what, limit)}
}

// policyStatements returns the statements of a policy ParseResource
// accepted, as their elements.
func policyStatements(doc string) ([]map[string]json.RawMessage, error) {
	var top struct {
		Statement json.RawMessage `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(doc), &top); err != nil {
		return nil, fmt.Errorf("decode policy: %w", err)
	}
	raw := bytes.TrimSpace(top.Statement)
	if len(raw) > 0 && raw[0] == '{' {
		var one map[string]json.RawMessage
		if err := json.Unmarshal(raw, &one); err != nil {
			return nil, fmt.Errorf("decode statement: %w", err)
		}
		return []map[string]json.RawMessage{one}, nil
	}
	var many []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, fmt.Errorf("decode statements: %w", err)
	}
	return many, nil
}

// valueCount is the number of values of an element that is a string or an
// array of them: 0 when it is absent.
func valueCount(raw json.RawMessage) int {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0
	}
	var many []json.RawMessage
	if json.Unmarshal(raw, &many) == nil {
		return len(many)
	}
	return 1
}

// principalCount is the number of principals a Principal or NotPrincipal
// names: 1 for "*", else the values under each principal type.
func principalCount(raw json.RawMessage) int {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0
	}
	var byType map[string]json.RawMessage
	if json.Unmarshal(raw, &byType) != nil {
		return 1
	}
	n := 0
	for _, v := range byType {
		n += valueCount(v)
	}
	return n
}

// conditionCount is the number of condition keys under every operator of a
// Condition.
func conditionCount(raw json.RawMessage) int {
	var ops map[string]map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) == 0 || json.Unmarshal(raw, &ops) != nil {
		return 0
	}
	n := 0
	for _, keys := range ops {
		n += len(keys)
	}
	return n
}
