//go:build e2e

package e2e

import (
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

// These tests run queues against real accounts and identity processes and
// check that a queue's policy is evaluated with the caller's identity
// policies as AWS evaluates them: within the management account, and for
// principals of a member account, whose users and sessions identity
// creates through the member's OrganizationAccountAccessRole session.

// orgStack is an identity stack with accounts behind it and a member
// account.
type orgStack struct {
	*identityStack
	// memberIAM is an IAM client of the member account, signing with its
	// OrganizationAccountAccessRole session.
	memberIAM *iam.Client
	// memberSession is that session's credentials.
	memberSession aws.Credentials
	member        string
}

func startOrgStack(t *testing.T) *orgStack {
	t.Helper()
	// The processes start before the app: startServer sets TULE_ variables
	// in this process, and envWithoutTule keeps them out of the children.
	accountsPort := identityFreePort(t)
	accountsDir := t.TempDir()
	startProcess(t, "accounts", buildService(t, "accounts", accountsDirEnv), accountsDir,
		accountsPort, []string{
			"TULE_Server_Port=" + strconv.Itoa(accountsPort),
			"TULE_Store_Path=" + filepath.Join(accountsDir, "accounts.db"),
		})
	accountsURL := "http://127.0.0.1:" + strconv.Itoa(accountsPort)
	member := createMemberAccount(t, accountsURL)

	identityPort := identityFreePort(t)
	identityURL := "http://127.0.0.1:" + strconv.Itoa(identityPort)
	s := &orgStack{
		identityStack: &identityStack{
			identityURL: identityURL,
			rootCreds: aws.Credentials{
				AccessKeyID: identityRootKeyID, SecretAccessKey: identityRootSecret,
			},
		},
		member: member,
	}
	s.identity = startIdentity(t, identityPort, "TULE_Identity_AccountsEndpoint="+accountsURL)
	s.srv = startServer(t, map[string]string{
		"TULE_Auth_AccessKeyID":      identityRootKeyID,
		"TULE_Auth_SecretAccessKey":  identityRootSecret,
		"TULE_Auth_IdentityEndpoint": identityURL,
	})
	s.iam = iamClient(identityURL, s.rootCreds)
	s.memberSession = assumeOrganizationRole(t, identityURL, member)
	s.memberIAM = iamClient(identityURL, s.memberSession)
	return s
}

// memberUser creates a user of the member account with doc as its policy,
// "" for none, and returns an SQS client signing with its key.
func (s *orgStack) memberUser(t *testing.T, name, doc string) *sqs.Client {
	t.Helper()
	key := createUserKey(t, s.memberIAM, name)
	if doc != "" {
		putUserPolicy(t, s.memberIAM, name, doc)
	}
	return sqsClientWith(s.srv, key)
}

func (s *orgStack) memberUserARN(name string) string {
	return "arn:aws:iam::" + s.member + ":user/" + name
}

// grant is a queue policy whose one statement has effect for principal (a
// JSON value) on actions (a JSON value) of the queue name, with condition
// (a JSON object, "" for none).
func grant(effect, principal, actions, name, condition string) string {
	st := `{"Sid":"Grant","Effect":"` + effect + `","Principal":` + principal +
		`,"Action":` + actions + `,"Resource":"` + queueARN(name) + `"`
	if condition != "" {
		st += `,"Condition":` + condition
	}
	return `{"Version":"2012-10-17","Statement":[` + st + `}]}`
}

// requireResourceDenied fails unless err is AccessDenied for principal,
// action and resource ending in tail, the reason's phrase.
func requireResourceDenied(t *testing.T, err error, principal, action, resource, tail string) {
	t.Helper()
	requireCode(t, err, "AccessDenied")
	requireStatus(t, err, http.StatusForbidden)
	action = strings.ToLower(action)
	want := "User: " + principal + " is not authorized to perform: " + action +
		" on resource: " + resource + " " + strings.ReplaceAll(tail, "<action>", action)
	var apiErr smithy.APIError
	if errors.As(err, &apiErr); apiErr.ErrorMessage() != want {
		t.Fatalf("message:\n got %s\nwant %s", apiErr.ErrorMessage(), want)
	}
}

const (
	noResourceAllow = "because no resource-based policy allows the <action> action"
	noIdentityAllow = "because no identity-based policy allows the <action> action"
	resourceDeny    = "with an explicit deny in a resource-based policy"
)

func sendTo(u string) *sqs.SendMessageInput {
	return &sqs.SendMessageInput{QueueUrl: aws.String(u), MessageBody: aws.String("x")}
}

func TestQueuePolicies(t *testing.T) {
	s := startOrgStack(t)

	t.Run("another account's user needs the queue policy", s.testCrossAccountUser)
	t.Run("another account's role session polls through the queue policy",
		s.testCrossAccountSession)
	t.Run("the owner-only operations are not shared across accounts", s.testOwnerOnly)
	t.Run("a Deny in the queue policy beats an identity Allow", s.testResourceDeny)
	t.Run("a grant naming a user of the account is enough alone", s.testSameAccountUser)
	t.Run("a grant to the account needs the identity Allow", s.testAccountGrant)
	t.Run("a role ARN with a path grants its sessions", s.testRoleWithPath)
	t.Run("aws:PrincipalTag is a condition key", s.testPrincipalTag)
	t.Run("the root key is never evaluated", s.testRootKey)
}

func (s *orgStack) testCrossAccountUser(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "cross-user", nil)
	c := s.memberUser(t, "producer", allowPolicy(`"sqs:SendMessage"`, "*"))
	arn := s.memberUserARN("producer")

	requireResourceDenied(t, errOf(c.SendMessage(t.Context(), sendTo(u))), arn,
		"sqs:SendMessage", queueARN("cross-user"), noResourceAllow)

	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `{"AWS":"`+arn+`"}`,
		`"sqs:SendMessage"`, "cross-user", "")), "grant the IAM user")
	send(t, c, u, "across accounts")

	// The grant does not reach an action it does not name.
	requireResourceDenied(t, errOf(c.ReceiveMessage(t.Context(),
		&sqs.ReceiveMessageInput{QueueUrl: aws.String(u)})), arn,
		"sqs:ReceiveMessage", queueARN("cross-user"), noIdentityAllow)

	// Nor is the queue policy enough without the identity Allow.
	bare := s.memberUser(t, "bare", "")
	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `{"AWS":"`+s.member+`"}`,
		`"sqs:SendMessage"`, "cross-user", "")), "grant the member account")
	requireResourceDenied(t, errOf(bare.SendMessage(t.Context(), sendTo(u))),
		s.memberUserARN("bare"), "sqs:SendMessage", queueARN("cross-user"), noIdentityAllow)
	send(t, c, u, "through the account grant")
}

func (s *orgStack) testCrossAccountSession(t *testing.T) {
	// functions polls an event source mapping's queue with its execution
	// role's session: ReceiveMessage, DeleteMessage and GetQueueAttributes.
	root := s.rootClient()
	u := createQueue(t, root, "cross-session", nil)
	send(t, root, u, "for the mapping")
	c := sqsClientWith(s.srv, s.memberSession)
	sessionARN := "arn:aws:sts::" + s.member + ":assumed-role/" + organizationRole + "/e2e"

	requireResourceDenied(t, errOf(c.ReceiveMessage(t.Context(),
		&sqs.ReceiveMessageInput{QueueUrl: aws.String(u)})), sessionARN,
		"sqs:ReceiveMessage", queueARN("cross-session"), noResourceAllow)

	roleARN := "arn:aws:iam::" + s.member + ":role/" + organizationRole
	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `{"AWS":"`+roleARN+`"}`,
		`["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:GetQueueAttributes"]`,
		"cross-session", "")), "grant the role")
	queueAttrs(t, c, u, "QueueArn")
	m := receiveOne(t, c, u)
	requireEqual(t, aws.ToString(m.Body), "for the mapping", "body")
	deleteMessage(t, c, u, m)
	requireCounts(t, root, u, counts{}, "after the session's delete")
}

func (s *orgStack) testOwnerOnly(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "owner-only", nil)
	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `"*"`, `"sqs:*"`, "owner-only", "")),
		"grant everything to everyone")
	c := sqsClientWith(s.srv, s.memberSession)
	sessionARN := "arn:aws:sts::" + s.member + ":assumed-role/" + organizationRole + "/e2e"

	// "Cross-account permissions don't apply to" these (SQS Developer
	// Guide), whatever the queue policy says.
	requireResourceDenied(t, errOf(c.SetQueueAttributes(t.Context(),
		&sqs.SetQueueAttributesInput{
			QueueUrl: aws.String(u), Attributes: map[string]string{"DelaySeconds": "5"},
		})), sessionARN, "sqs:SetQueueAttributes", queueARN("owner-only"), noResourceAllow)
	requireResourceDenied(t, errOf(c.DeleteQueue(t.Context(),
		&sqs.DeleteQueueInput{QueueUrl: aws.String(u)})), sessionARN,
		"sqs:DeleteQueue", queueARN("owner-only"), noResourceAllow)
	requireResourceDenied(t, errOf(c.AddPermission(t.Context(), &sqs.AddPermissionInput{
		QueueUrl: aws.String(u), Label: aws.String("mine"),
		AWSAccountIds: []string{s.member}, Actions: []string{"*"},
	})), sessionARN, "sqs:AddPermission", queueARN("owner-only"), noResourceAllow)
	// A shared action is granted.
	send(t, c, u, "shared")
}

func (s *orgStack) testResourceDeny(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "denied", nil)
	c, _ := s.policyUser(t, "allowed-all", allowAllQueuesPolicy)
	send(t, c, u, "before the deny")
	requireNoErr(t, setPolicy(t, root, u, grant("Deny", `{"AWS":"`+userARN("allowed-all")+`"}`,
		`"sqs:SendMessage"`, "denied", "")), "deny the IAM user")
	requireResourceDenied(t, errOf(c.SendMessage(t.Context(), sendTo(u))),
		userARN("allowed-all"), "sqs:SendMessage", queueARN("denied"), resourceDeny)
}

func (s *orgStack) testSameAccountUser(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "direct", nil)
	c, _ := s.policyUser(t, "no-identity-policy", "")
	requireResourceDenied(t, errOf(c.SendMessage(t.Context(), sendTo(u))),
		userARN("no-identity-policy"), "sqs:SendMessage", queueARN("direct"), noIdentityAllow)
	requireNoErr(t, setPolicy(t, root, u, grant("Allow",
		`{"AWS":"`+userARN("no-identity-policy")+`"}`, `"sqs:SendMessage"`, "direct", "")),
		"grant the IAM user")
	send(t, c, u, "granted by the queue policy alone")
}

func (s *orgStack) testAccountGrant(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "account-grant", nil)
	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `{"AWS":"`+defaultAccountID+`"}`,
		`"sqs:SendMessage"`, "account-grant", "")), "grant the account")
	bare, _ := s.policyUser(t, "account-bare", "")
	requireResourceDenied(t, errOf(bare.SendMessage(t.Context(), sendTo(u))),
		userARN("account-bare"), "sqs:SendMessage", queueARN("account-grant"), noIdentityAllow)
	allowed, _ := s.policyUser(t, "account-allowed", allowPolicy(`"sqs:SendMessage"`, "*"))
	send(t, allowed, u, "with the identity Allow")
}

// assumePathRole creates the role name at path /jobs/ with tags, trusting
// a new user of the account, and returns a session's SQS client and ARN
// and the role's ARN.
func (s *orgStack) assumePathRole(
	t *testing.T, name string, tags map[string]string,
) (*sqs.Client, string, string) {
	t.Helper()
	assumer := name + "-assumer"
	key := createUserKey(t, s.iam, assumer)
	roleARN := "arn:aws:iam::" + defaultAccountID + ":role/jobs/" + name
	putUserPolicy(t, s.iam, assumer, allowPolicy(`"sts:AssumeRole"`, roleARN))
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		"Principal":{"AWS":"` + userARN(assumer) + `"},"Action":"sts:AssumeRole"}]}`
	in := &iam.CreateRoleInput{
		RoleName: aws.String(name), Path: aws.String("/jobs/"),
		AssumeRolePolicyDocument: aws.String(trust),
	}
	for k, v := range tags {
		in.Tags = append(in.Tags, iamtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	_, err := s.iam.CreateRole(t.Context(), in)
	requireNoErr(t, err, "create the role "+name)
	sc := sts.New(sts.Options{
		Region: region, BaseEndpoint: aws.String(s.identityURL),
		Credentials:      credentials.StaticCredentialsProvider{Value: key},
		HTTPClient:       &http.Client{Timeout: clientTimeout},
		RetryMaxAttempts: 1,
	})
	out, err := sc.AssumeRole(t.Context(), &sts.AssumeRoleInput{
		RoleArn: aws.String(roleARN), RoleSessionName: aws.String("job"),
	})
	requireNoErr(t, err, "assume the role "+name)
	c := sqsClientWith(s.srv, aws.Credentials{
		AccessKeyID:     aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(out.Credentials.SessionToken),
	})
	return c, "arn:aws:sts::" + defaultAccountID + ":assumed-role/" + name + "/job", roleARN
}

func (s *orgStack) testRoleWithPath(t *testing.T) {
	root := s.rootClient()
	c, sessionARN, roleARN := s.assumePathRole(t, "path-worker", nil)

	u := createQueue(t, root, "role-grant", nil)
	requireResourceDenied(t, errOf(c.SendMessage(t.Context(), sendTo(u))), sessionARN,
		"sqs:SendMessage", queueARN("role-grant"), noIdentityAllow)
	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `{"AWS":"`+roleARN+`"}`,
		`"sqs:SendMessage"`, "role-grant", "")), "grant the role by its ARN with its path")
	send(t, c, u, "a session of the granted role")

	// aws:PrincipalArn is the role's ARN with its path.
	v := createQueue(t, root, "path-condition", nil)
	requireNoErr(t, setPolicy(t, root, v, grant("Allow", `"*"`, `"sqs:SendMessage"`,
		"path-condition", `{"ArnLike":{"aws:PrincipalArn":"arn:aws:iam::*:role/jobs/*"}}`)),
		"grant on the path")
	send(t, c, v, "matched by its path")
	w := createQueue(t, root, "path-deny", nil)
	requireNoErr(t, setPolicy(t, root, w, `{"Version":"2012-10-17","Statement":[`+
		`{"Effect":"Allow","Principal":"*","Action":"sqs:SendMessage","Resource":"`+
		queueARN("path-deny")+`"},{"Effect":"Deny","Principal":"*","Action":"sqs:*",`+
		`"Resource":"`+queueARN("path-deny")+`","Condition":{"ArnLike":`+
		`{"aws:PrincipalArn":"arn:aws:iam::*:role/jobs/*"}}}]}`), "deny on the path")
	requireResourceDenied(t, errOf(c.SendMessage(t.Context(), sendTo(w))), sessionARN,
		"sqs:SendMessage", queueARN("path-deny"), resourceDeny)
}

func (s *orgStack) testPrincipalTag(t *testing.T) {
	root := s.rootClient()
	blue, _, _ := s.assumePathRole(t, "blue-worker", map[string]string{"team": "blue"})
	red, redARN, _ := s.assumePathRole(t, "red-worker", map[string]string{"team": "red"})
	u := createQueue(t, root, "blue-only", nil)
	requireNoErr(t, setPolicy(t, root, u, grant("Allow", `"*"`, `"sqs:SendMessage"`,
		"blue-only", `{"StringEquals":{"aws:PrincipalTag/team":"blue"}}`)), "grant on a tag")
	send(t, blue, u, "tagged blue")
	requireResourceDenied(t, errOf(red.SendMessage(t.Context(), sendTo(u))), redARN,
		"sqs:SendMessage", queueARN("blue-only"), noIdentityAllow)
}
