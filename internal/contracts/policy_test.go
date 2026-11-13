package contracts

import (
	"strconv"
	"strings"
	"testing"
)

const queue = "arn:aws:sqs:us-east-1:000000000000:queue1"

func statementWith(sid, principal, action, condition string) string {
	st := `{"Sid":"` + sid + `","Effect":"Allow","Principal":` + principal + `,"Action":` +
		action + `,"Resource":"` + queue + `"`
	if condition != "" {
		st += `,"Condition":` + condition
	}
	return st + "}"
}

func policyOf(statements ...string) string {
	return `{"Version":"2012-10-17","Statement":[` + strings.Join(statements, ",") + `]}`
}

func TestCheckPolicyTakesTheDeveloperGuidesExample(t *testing.T) {
	// "Basic examples of Amazon SQS policies", Example 1.
	doc := `{"Version":"2012-10-17","Id":"Queue1_Policy_UUID","Statement":[{` +
		`"Sid":"Queue1_SendMessage","Effect":"Allow","Principal":{"AWS":["111122223333"]},` +
		`"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:us-east-2:444455556666:queue1"}]}`
	if pe := CheckPolicy(doc); pe != nil {
		t.Fatalf("CheckPolicy = %v", pe)
	}
	// A single statement object is one statement.
	single := `{"Statement":` + statementWith("a", `"*"`, `"sqs:*"`, "") + `}`
	if pe := CheckPolicy(single); pe != nil {
		t.Fatalf("a single statement: %v", pe)
	}
}

func TestCheckPolicyRefusesWhatItCannotEvaluate(t *testing.T) {
	for name, doc := range map[string]string{
		"not JSON":     `{"Statement":`,
		"no Principal": `{"Statement":{"Effect":"Allow","Action":"sqs:*","Resource":"*"}}`,
		"no Resource":  `{"Statement":{"Effect":"Allow","Principal":"*","Action":"sqs:*"}}`,
	} {
		pe := CheckPolicy(doc)
		if pe == nil || pe.OverLimit || pe.Reason == "" {
			t.Errorf("%s: %+v, want a malformed-policy error", name, pe)
		}
	}
}

func TestCheckPolicyAppliesTheQuotas(t *testing.T) {
	many := func(n int, each func(int) string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = each(i)
		}
		return out
	}
	accounts := func(n int) string {
		ids := many(n, func(i int) string { return `"` + strconv.Itoa(100000000000+i) + `"` })
		return `{"AWS":[` + strings.Join(ids, ",") + `]}`
	}
	conditions := func(n int) string {
		keys := many(
			n,
			func(i int) string { return `"aws:PrincipalTag/k` + strconv.Itoa(i) + `":"v"` },
		)
		return `{"StringEquals":{` + strings.Join(keys, ",") + `}}`
	}
	actions := func(n int) string {
		names := many(n, func(i int) string { return `"sqs:A` + strconv.Itoa(i) + `"` })
		return "[" + strings.Join(names, ",") + "]"
	}
	plain := func(i int) string { return statementWith("s"+strconv.Itoa(i), `"*"`, `"sqs:*"`, "") }
	for name, tc := range map[string]struct {
		doc  string
		over bool
	}{
		"20 statements":        {policyOf(many(20, plain)...), false},
		"21 statements":        {policyOf(many(21, plain)...), true},
		"7 actions":            {policyOf(statementWith("a", `"*"`, actions(7), "")), false},
		"8 actions":            {policyOf(statementWith("a", `"*"`, actions(8), "")), true},
		"50 principals":        {policyOf(statementWith("a", accounts(50), `"sqs:*"`, "")), false},
		"51 across statements": {policyOf(statementWith("a", accounts(50), `"sqs:*"`, ""), plain(1)), true},
		"10 conditions":        {policyOf(statementWith("a", `"*"`, `"sqs:*"`, conditions(10))), false},
		"11 conditions":        {policyOf(statementWith("a", `"*"`, `"sqs:*"`, conditions(11))), true},
		"over 8,192 bytes": {policyOf(statementWith("a", `"*"`, `"sqs:*"`,
			`{"StringEquals":{"aws:userid":"`+strings.Repeat("x", MaxPolicySize)+`"}}`)), true},
	} {
		pe := CheckPolicy(tc.doc)
		switch {
		case tc.over && (pe == nil || !pe.OverLimit):
			t.Errorf("%s: %+v, want over a quota", name, pe)
		case !tc.over && pe != nil:
			t.Errorf("%s: %v, want none", name, pe)
		}
	}
}
