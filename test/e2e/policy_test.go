//go:build e2e

package e2e

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

// These tests run queues against a real identity and check that the keys
// identity issues are evaluated against their identity policies, while the
// root key is not.

// allowAllQueuesPolicy allows every SQS action and the inspection API.
const allowAllQueuesPolicy = `{"Version":"2012-10-17","Statement":[
	{"Effect":"Allow","Action":["sqs:*","tule:Inspect"],"Resource":"*"}]}`

// queueARN is the ARN of the queue name in the default account and region.
func queueARN(name string) string {
	return "arn:aws:sqs:" + region + ":" + defaultAccountID + ":" + name
}

func userARN(name string) string {
	return "arn:aws:iam::" + defaultAccountID + ":user/" + name
}

// putUserPolicy puts doc as an inline policy of user.
func putUserPolicy(t *testing.T, ic *iam.Client, user, doc string) {
	t.Helper()
	_, err := ic.PutUserPolicy(t.Context(), &iam.PutUserPolicyInput{
		UserName:       aws.String(user),
		PolicyName:     aws.String("queues"),
		PolicyDocument: aws.String(doc),
	})
	requireNoErr(t, err, "put the policy of "+user)
}

// policyUser creates a user with doc as its policy, "" for none, and returns
// an SQS client signing with its key.
func (s *identityStack) policyUser(t *testing.T, name, doc string) (*sqs.Client, aws.Credentials) {
	t.Helper()
	key := createUserKey(t, s.iam, name)
	if doc != "" {
		putUserPolicy(t, s.iam, name, doc)
	}
	return sqsClientWith(s.srv, key), key
}

func (s *identityStack) rootClient() *sqs.Client {
	return sqsClientWith(s.srv, s.rootCreds)
}

func allowPolicy(actions, resource string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":` + actions +
		`,"Resource":"` + resource + `"}]}`
}

// requireAccessDenied fails unless err is SQS's AccessDenied, 403, with
// IAM's message form for principal, action and resource; SQS writes the
// action in lower case.
func requireAccessDenied(
	t *testing.T,
	err error,
	principal, action, resource string,
	explicit bool,
) {
	t.Helper()
	requireCode(t, err, "AccessDenied")
	requireStatus(t, err, http.StatusForbidden)
	action = strings.ToLower(action)
	want := "User: " + principal + " is not authorized to perform: " + action +
		" on resource: " + resource
	if explicit {
		want += " with an explicit deny in an identity-based policy"
	} else {
		want += " because no identity-based policy allows the " + action + " action"
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr); apiErr.ErrorMessage() != want {
		t.Fatalf("message:\n got %s\nwant %s", apiErr.ErrorMessage(), want)
	}
}

// requireCrossAccountDenied fails unless err is AccessDenied for a
// principal of another account that its identity policies allow, which
// needs a resource-based policy as well.
func requireCrossAccountDenied(t *testing.T, err error, principal, action, resource string) {
	t.Helper()
	requireCode(t, err, "AccessDenied")
	requireStatus(t, err, http.StatusForbidden)
	action = strings.ToLower(action)
	want := "User: " + principal + " is not authorized to perform: " + action +
		" on resource: " + resource + " because no resource-based policy allows the " +
		action + " action"
	var apiErr smithy.APIError
	if errors.As(err, &apiErr); apiErr.ErrorMessage() != want {
		t.Fatalf("message:\n got %s\nwant %s", apiErr.ErrorMessage(), want)
	}
}

func TestIdentityPolicies(t *testing.T) {
	s := startIdentityStack(t)
	createQueue(t, s.rootClient(), "orders", nil)
	createQueue(t, s.rootClient(), "other", nil)

	t.Run("a user with no policy is denied", s.testNoPolicy)
	t.Run("a policy for one action allows only that action", s.testOneAction)
	t.Run("a resource-scoped policy allows its queue only", s.testResourceScoped)
	t.Run("an explicit deny beats an allow", s.testExplicitDeny)
	t.Run("aws:SourceIp is honored", s.testSourceIP)
	t.Run("aws:RequestTag is honored", s.testRequestTag)
	t.Run("a role session gets its role's permissions", s.testRoleSession)

	t.Run("the root key is always allowed", s.testRootKey)
}

func (s *identityStack) testNoPolicy(t *testing.T) {
	c, key := s.policyUser(t, "nobody", "")
	err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(s.srv.queueURL("orders")), MessageBody: aws.String("x"),
	}))
	requireAccessDenied(t, err, userARN("nobody"), "sqs:SendMessage", queueARN("orders"), false)
	err = errOf(c.ListQueues(t.Context(), &sqs.ListQueuesInput{}))
	requireAccessDenied(t, err, userARN("nobody"), "sqs:ListQueues", "*", false)
	requireAPIErrorBody(
		t,
		inspectorWith(s.srv, key).do(t, http.MethodGet, "/_queues/queues", nil),
		http.StatusForbidden,
		"AccessDenied",
		"inspection without tule:Inspect",
	)
}

func (s *identityStack) testOneAction(t *testing.T) {
	c, _ := s.policyUser(t, "sender", allowPolicy(`"sqs:SendMessage"`, "*"))
	send(t, c, s.srv.queueURL("orders"), "from sender")
	_, err := c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(s.srv.queueURL("orders")),
		Entries:  batchEntries("one", "two"),
	})
	requireNoErr(t, err, "SendMessageBatch, which is sqs:SendMessage")
	err = errOf(
		c.ReceiveMessage(
			t.Context(),
			&sqs.ReceiveMessageInput{QueueUrl: aws.String(s.srv.queueURL("orders"))},
		),
	)
	requireAccessDenied(
		t,
		err,
		userARN("sender"),
		"sqs:ReceiveMessage",
		queueARN("orders"),
		false,
	)
	err = errOf(
		c.GetQueueAttributes(
			t.Context(),
			&sqs.GetQueueAttributesInput{QueueUrl: aws.String(s.srv.queueURL("orders"))},
		),
	)
	requireAccessDenied(
		t,
		err,
		userARN("sender"),
		"sqs:GetQueueAttributes",
		queueARN("orders"),
		false,
	)
}

func (s *identityStack) testResourceScoped(t *testing.T) {
	c, _ := s.policyUser(t, "scoped", allowPolicy(`"sqs:*"`, queueARN("orders")))
	send(t, c, s.srv.queueURL("orders"), "scoped")
	err := errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(s.srv.queueURL("other")), MessageBody: aws.String("x"),
	}))
	requireAccessDenied(t, err, userARN("scoped"), "sqs:SendMessage", queueARN("other"), false)
}

func (s *identityStack) testExplicitDeny(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"sqs:*","Resource":"*"},
		{"Effect":"Deny","Action":"sqs:DeleteQueue","Resource":"` + queueARN("orders") + `"}]}`
	c, _ := s.policyUser(t, "denied", doc)
	err := errOf(
		c.DeleteQueue(
			t.Context(),
			&sqs.DeleteQueueInput{QueueUrl: aws.String(s.srv.queueURL("orders"))},
		),
	)
	requireAccessDenied(t, err, userARN("denied"), "sqs:DeleteQueue", queueARN("orders"), true)
	queueAttrs(t, s.rootClient(), s.srv.queueURL("orders"), "QueueArn")
}

func batchEntries(bodies ...string) []types.SendMessageBatchRequestEntry {
	out := make([]types.SendMessageBatchRequestEntry, 0, len(bodies))
	for i, b := range bodies {
		out = append(
			out,
			types.SendMessageBatchRequestEntry{
				Id:          aws.String("e" + strconv.Itoa(i)),
				MessageBody: aws.String(b),
			},
		)
	}
	return out
}

func (s *identityStack) testSourceIP(t *testing.T) {
	cond := func(cidr string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:ListQueues",
			"Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"` + cidr + `"}}}]}`
	}
	local, _ := s.policyUser(t, "local", cond("127.0.0.0/8"))
	_, err := local.ListQueues(t.Context(), &sqs.ListQueuesInput{})
	requireNoErr(t, err, "list queues from 127.0.0.1")

	remote, _ := s.policyUser(t, "remote", cond("192.0.2.0/24"))
	err = errOf(remote.ListQueues(t.Context(), &sqs.ListQueuesInput{}))
	requireAccessDenied(t, err, userARN("remote"), "sqs:ListQueues", "*", false)
}

func (s *identityStack) testRequestTag(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		"Action":["sqs:CreateQueue","sqs:TagQueue"],"Resource":"*",
		"Condition":{"StringEquals":{"aws:RequestTag/team":"blue"}}}]}`
	c, _ := s.policyUser(t, "tagger", doc)
	_, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("blue"), Tags: map[string]string{"team": "blue"},
	})
	requireNoErr(t, err, "create a queue tagged team=blue")
	err = errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("red"), Tags: map[string]string{"team": "red"},
	}))
	requireAccessDenied(t, err, userARN("tagger"), "sqs:CreateQueue", queueARN("red"), false)
	requireQueueMissing(t, s.rootClient(), "red")
}

func (s *identityStack) testRoleSession(t *testing.T) {
	const role = "worker"
	roleARN := "arn:aws:iam::" + defaultAccountID + ":role/" + role
	createUserKey(t, s.iam, "assumer")
	putUserPolicy(t, s.iam, "assumer", allowPolicy(`"sts:AssumeRole"`, roleARN))
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		"Principal":{"AWS":"` + userARN("assumer") + `"},"Action":"sts:AssumeRole"}]}`
	_, err := s.iam.CreateRole(t.Context(), &iam.CreateRoleInput{
		RoleName: aws.String(role), AssumeRolePolicyDocument: aws.String(trust),
	})
	requireNoErr(t, err, "create the role")
	_, err = s.iam.PutRolePolicy(t.Context(), &iam.PutRolePolicyInput{
		RoleName: aws.String(role), PolicyName: aws.String("queues"),
		PolicyDocument: aws.String(allowPolicy(`["sqs:SendMessage","sqs:GetQueueUrl"]`,
			queueARN("orders"))),
	})
	requireNoErr(t, err, "put the role's policy")

	assumerKey, err := s.iam.CreateAccessKey(t.Context(),
		&iam.CreateAccessKeyInput{UserName: aws.String("assumer")})
	requireNoErr(t, err, "create the assumer's key")
	sc := sts.New(sts.Options{
		Region: region, BaseEndpoint: aws.String(s.identityURL),
		Credentials: credentials.StaticCredentialsProvider{Value: aws.Credentials{
			AccessKeyID:     aws.ToString(assumerKey.AccessKey.AccessKeyId),
			SecretAccessKey: aws.ToString(assumerKey.AccessKey.SecretAccessKey),
		}},
		HTTPClient: &http.Client{Timeout: clientTimeout}, RetryMaxAttempts: 1,
	})
	out, err := sc.AssumeRole(t.Context(), &sts.AssumeRoleInput{
		RoleArn: aws.String(roleARN), RoleSessionName: aws.String("job"),
	})
	requireNoErr(t, err, "assume the role")
	c := sqsClientWith(s.srv, aws.Credentials{
		AccessKeyID:     aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(out.Credentials.SessionToken),
	})

	send(t, c, s.srv.queueURL("orders"), "from the session")
	sessionARN := "arn:aws:sts::" + defaultAccountID + ":assumed-role/" + role + "/job"
	err = errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(s.srv.queueURL("other")), MessageBody: aws.String("x"),
	}))
	requireAccessDenied(t, err, sessionARN, "sqs:SendMessage", queueARN("other"), false)
	err = errOf(c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(s.srv.queueURL("orders")),
	}))
	requireAccessDenied(t, err, sessionARN, "sqs:ReceiveMessage", queueARN("orders"), false)
}

func TestStandaloneIsUnchanged(t *testing.T) {
	// No identity endpoint: the static key is the account's root user and
	// may do everything, the inspection API included.
	c, srv := newClient(t)
	u := createQueue(t, c, "standalone", nil)
	send(t, c, u, "hello")
	_, err := c.TagQueue(t.Context(), &sqs.TagQueueInput{
		QueueUrl: aws.String(u), Tags: map[string]string{"team": "blue"},
	})
	requireNoErr(t, err, "tag the queue")
	_, err = c.DeleteQueue(t.Context(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "delete the queue")
	requireOK(t, inspectorWith(srv, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}).
		do(t, http.MethodGet, "/_queues/queues", nil), "inspection with the static key")
}

func (s *identityStack) testRootKey(t *testing.T) {
	// The service's own static key is identity's root key here, as it
	// must be for queues to ask identity about keys: the verifier
	// matches it before asking identity.
	c := s.rootClient()
	u := createQueue(t, c, "root-only", map[string]string{"VisibilityTimeout": "5"})
	send(t, c, u, "root")
	deleteMessage(t, c, u, receiveOne(t, c, u))
	_, err := c.DeleteQueue(t.Context(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "delete with the root key")
	requireOK(t, inspectorWith(s.srv, s.rootCreds).do(t, http.MethodGet, "/_queues/queues", nil),
		"inspection with the root key")
}
