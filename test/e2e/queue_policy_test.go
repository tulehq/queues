//go:build e2e

package e2e

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// These tests drive the queue policy API, the Policy attribute and
// AddPermission and RemovePermission, with the SQS SDK and the root key.

// docsExamplePolicy is the SQS Developer Guide's "Example 1: Grant one
// permission to one AWS account" ("Basic examples of Amazon SQS
// policies"), with a Sid that has an underscore, for the queue name.
func docsExamplePolicy(name string) string {
	return `{
   "Version":"2012-10-17",
   "Id": "Queue1_Policy_UUID",
   "Statement": [{
      "Sid":"Queue1_SendMessage",
      "Effect": "Allow",
      "Principal": {
         "AWS": [
            "111122223333"
         ]
      },
      "Action": "sqs:SendMessage",
      "Resource": "` + queueARN(name) + `"
   }]
}`
}

func setPolicy(t *testing.T, c *sqs.Client, u, doc string) error {
	t.Helper()
	return errOf(c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(u),
		Attributes: map[string]string{string(types.QueueAttributeNamePolicy): doc},
	}))
}

func queuePolicyOf(t *testing.T, c *sqs.Client, u string) string {
	t.Helper()
	return queueAttrs(t, c, u, string(types.QueueAttributeNamePolicy))["Policy"]
}

func TestThePolicyAttribute(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "with-policy", nil)

	t.Run("the developer guide's example is stored as sent", func(t *testing.T) {
		doc := docsExamplePolicy("with-policy")
		requireNoErr(t, setPolicy(t, c, u, doc), "set the example policy")
		requireEqual(t, queuePolicyOf(t, c, u), doc, "Policy")
	})
	t.Run("an empty Policy removes it", func(t *testing.T) {
		requireNoErr(t, setPolicy(t, c, u, ""), "remove the policy")
		if p, ok := queueAttrs(t, c, u, "Policy")["Policy"]; ok {
			t.Fatalf("Policy after removal = %q", p)
		}
	})
	t.Run("CreateQueue takes one", func(t *testing.T) {
		doc := docsExamplePolicy("created-with-policy")
		v := createQueue(t, c, "created-with-policy", map[string]string{"Policy": doc})
		requireEqual(t, queuePolicyOf(t, c, v), doc, "Policy")
		err := errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
			QueueName:  aws.String("created-with-bad-policy"),
			Attributes: map[string]string{"Policy": `{"Statement":`},
		}))
		requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
		requireQueueMissing(t, c, "created-with-bad-policy")
	})
}

// policyStatement is an Allow of sqs:SendMessage on with-policy to every
// principal, with Sid s<i>.
func policyStatement(i int) string {
	return `{"Sid":"s` + strconv.Itoa(i) + `","Effect":"Allow","Principal":"*",` +
		`"Action":"sqs:SendMessage","Resource":"` + queueARN("with-policy") + `"}`
}

// policyStatements is a policy of n policyStatement.
func policyStatements(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = policyStatement(i)
	}
	return `{"Version":"2012-10-17","Statement":[` + strings.Join(parts, ",") + `]}`
}

func TestABadPolicyIsRefused(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "with-policy", nil)
	requireNoErr(t, setPolicy(t, c, u, docsExamplePolicy("with-policy")), "set a policy")
	for name, doc := range map[string]string{
		"not JSON": `{"Statement":`,
		"no Principal": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":"sqs:SendMessage","Resource":"*"}]}`,
		"a group as Principal": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"AWS":"arn:aws:iam::111122223333:group/dev"},` +
			`"Action":"sqs:SendMessage","Resource":"*"}]}`,
		"repeated Sids": `{"Version":"2012-10-17","Statement":[` + policyStatement(1) + "," +
			policyStatement(1) + `]}`,
		"over 8,192 bytes": `{"Version":"2012-10-17","Statement":[` +
			strings.Replace(policyStatement(1), `"s1"`, `"`+strings.Repeat("s", 8192)+`"`, 1) + `]}`,
		"21 statements": policyStatements(21),
		"8 actions": strings.Replace(policyStatements(1), `"sqs:SendMessage"`,
			`["sqs:SendMessage","sqs:ReceiveMessage","sqs:DeleteMessage",`+
				`"sqs:ChangeMessageVisibility","sqs:GetQueueAttributes","sqs:GetQueueUrl",`+
				`"sqs:PurgeQueue","sqs:ListQueueTags"]`, 1),
	} {
		t.Run("refused: "+name, func(t *testing.T) {
			before := queuePolicyOf(t, c, u)
			err := setPolicy(t, c, u, doc)
			requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
			requireEqual(t, queuePolicyOf(t, c, u), before, "Policy after the refusal")
		})
	}
	t.Run("20 statements are taken", func(t *testing.T) {
		requireNoErr(t, setPolicy(t, c, u, policyStatements(20)), "set 20 statements")
	})
}

func TestAddAndRemovePermission(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "shared", nil)
	add := func(label string, accounts, actions []string) error {
		return errOf(c.AddPermission(t.Context(), &sqs.AddPermissionInput{
			QueueUrl: aws.String(u), Label: aws.String(label),
			AWSAccountIds: accounts, Actions: actions,
		}))
	}

	t.Run("a statement as SQS generates it", func(t *testing.T) {
		requireNoErr(t, add("Send_to-orders", []string{"111122223333", "444455556666"},
			[]string{"SendMessage", "ReceiveMessage"}), "add a permission")
		want := `{"Version":"2012-10-17","Id":"` + queueARN("shared") + `/SQSDefaultPolicy",` +
			`"Statement":[{"Principal":{"AWS":["arn:aws:iam::111122223333:root",` +
			`"arn:aws:iam::444455556666:root"]},"Sid":"Send_to-orders","Effect":"Allow",` +
			`"Resource":"` + queueARN("shared") + `","Action":["SQS:SendMessage",` +
			`"SQS:ReceiveMessage"]}]}`
		requireEqual(t, queuePolicyOf(t, c, u), want, "Policy")
	})
	t.Run("a label in use is refused", func(t *testing.T) {
		requireInvalidParameter(t, add("Send_to-orders", []string{"111122223333"},
			[]string{"SendMessage"}))
	})
	t.Run("past 20 statements is OverLimit", func(t *testing.T) {
		for i := range 19 {
			requireNoErr(t, add("p"+strconv.Itoa(i), []string{"111122223333"},
				[]string{"SendMessage"}), "add permission "+strconv.Itoa(i))
		}
		err := add("p19", []string{"111122223333"}, []string{"SendMessage"})
		requireModeled[*types.OverLimit](t, err, "OverLimit")
	})
	t.Run("RemovePermission removes the label's statement", func(t *testing.T) {
		_, err := c.RemovePermission(t.Context(), &sqs.RemovePermissionInput{
			QueueUrl: aws.String(u), Label: aws.String("Send_to-orders"),
		})
		requireNoErr(t, err, "remove the permission")
		var doc struct{ Statement []struct{ Sid string } }
		requireNoErr(t, json.Unmarshal([]byte(queuePolicyOf(t, c, u)), &doc), "decode Policy")
		for _, st := range doc.Statement {
			if st.Sid == "Send_to-orders" {
				t.Fatal("the removed statement is still there")
			}
		}
		requireEqual(t, len(doc.Statement), 19, "statements left")
		requireInvalidParameter(t, errOf(c.RemovePermission(t.Context(),
			&sqs.RemovePermissionInput{
				QueueUrl: aws.String(u), Label: aws.String("Send_to-orders"),
			})))
	})
}

func TestAddPermissionChecksItsRequest(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "checked", nil)
	add := func(label string, accounts, actions []string) error {
		return errOf(c.AddPermission(t.Context(), &sqs.AddPermissionInput{
			QueueUrl: aws.String(u), Label: aws.String(label),
			AWSAccountIds: accounts, Actions: actions,
		}))
	}
	t.Run("the request is checked", func(t *testing.T) {
		requireInvalidParameter(t, add("bad label", []string{"111122223333"},
			[]string{"SendMessage"}))
		requireInvalidParameter(t, add(strings.Repeat("l", 81), []string{"111122223333"},
			[]string{"SendMessage"}))
		requireInvalidParameter(t, add("short-account", []string{"1111"},
			[]string{"SendMessage"}))
		requireInvalidParameter(t, add("unknown-action", []string{"111122223333"},
			[]string{"Publish"}))
		err := add("eight", []string{"111122223333"}, []string{
			"SendMessage", "ReceiveMessage",
			"DeleteMessage", "ChangeMessageVisibility", "GetQueueAttributes", "GetQueueUrl",
			"PurgeQueue", "ListQueueTags",
		})
		requireModeled[*types.OverLimit](t, err, "OverLimit")
	})
}

func TestAddPermissionKeepsAPolicySetBySetQueueAttributes(t *testing.T) {
	c, _ := newClient(t)
	v := createQueue(t, c, "shared-example", nil)
	requireNoErr(t, setPolicy(t, c, v, docsExamplePolicy("shared-example")), "set")
	_, err := c.AddPermission(t.Context(), &sqs.AddPermissionInput{
		QueueUrl: aws.String(v), Label: aws.String("more"),
		AWSAccountIds: []string{"444455556666"}, Actions: []string{"*"},
	})
	requireNoErr(t, err, "add a permission")
	var doc struct {
		ID        string `json:"Id"`
		Statement []struct{ Sid string }
	}
	requireNoErr(t, json.Unmarshal([]byte(queuePolicyOf(t, c, v)), &doc), "decode")
	requireEqual(t, doc.ID, "Queue1_Policy_UUID", "Id")
	requireEqual(t, len(doc.Statement), 2, "statements")
	requireEqual(t, doc.Statement[0].Sid, "Queue1_SendMessage", "first Sid")
	requireEqual(t, doc.Statement[1].Sid, "more", "second Sid")
}
