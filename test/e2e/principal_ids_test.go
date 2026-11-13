//go:build e2e

package e2e

import (
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	awsorg "github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// These tests check, against real identity and accounts processes, that a
// queue policy's user and role ARNs are bound to unique IDs when it is
// stored and shown as ARNs when it is read, and that aws:PrincipalOrgID
// and aws:PrincipalOrgPaths reach evaluation.

// uniqueID is the shape of a user's unique ID.
var uniqueID = regexp.MustCompile(`^AIDA[A-Z0-9]{17}$`)

func TestUniqueIDPrincipals(t *testing.T) {
	s := startIdentityStack(t)

	t.Run("a user created again under a granted ARN is not granted",
		s.testRecreatedUser)
	t.Run("a policy naming a user that does not exist is refused", s.testMissingPrincipal)
}

// userID returns the unique ID of the IAM user name.
func (s *identityStack) userID(t *testing.T, name string) string {
	t.Helper()
	out, err := s.iam.GetUser(t.Context(), &iam.GetUserInput{UserName: aws.String(name)})
	requireNoErr(t, err, "get user "+name)
	return aws.ToString(out.User.UserId)
}

// deleteUser deletes the IAM user name and its access key.
func (s *identityStack) deleteUser(t *testing.T, name string, key aws.Credentials) {
	t.Helper()
	_, err := s.iam.DeleteAccessKey(t.Context(), &iam.DeleteAccessKeyInput{
		UserName: aws.String(name), AccessKeyId: aws.String(key.AccessKeyID),
	})
	requireNoErr(t, err, "delete the access key of "+name)
	_, err = s.iam.DeleteUser(t.Context(), &iam.DeleteUserInput{UserName: aws.String(name)})
	requireNoErr(t, err, "delete user "+name)
}

func (s *identityStack) testRecreatedUser(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "recreated", nil)
	key := createUserKey(t, s.iam, "recreated")
	oldID := s.userID(t, "recreated")
	requireMatch(t, uniqueID, oldID, "the IAM user's ID")

	// The IAM user has no identity policy: the queue policy alone grants it.
	doc := grant("Allow", `{"AWS":"`+userARN("recreated")+`"}`, `"sqs:SendMessage"`,
		"recreated", "")
	requireNoErr(t, setPolicy(t, root, u, doc), "grant the IAM user")
	send(t, sqsClientWith(s.srv, key), u, "granted")

	// While the IAM user exists, the policy is shown as sent, naming its ARN,
	// by GetQueueAttributes and the inspection API.
	requireEqual(t, queuePolicyOf(t, root, u), doc, "Policy while the IAM user exists")
	requireEqual(t, inspectedPolicy(t, s.srv, s.rootCreds, "recreated"), doc,
		"inspected Policy while the IAM user exists")

	s.deleteUser(t, "recreated", key)
	newKey := createUserKey(t, s.iam, "recreated")
	if s.userID(t, "recreated") == oldID {
		t.Fatal("the recreated user has the deleted one's ID")
	}

	requireResourceDenied(t, errOf(sqsClientWith(s.srv, newKey).SendMessage(t.Context(),
		sendTo(u))), userARN("recreated"), "sqs:SendMessage", queueARN("recreated"),
		noIdentityAllow)

	// The policy names the deleted user, by the ID AWS shows once it "can no
	// longer map it back to a valid ARN".
	shown := grant("Allow", `{"AWS":"`+oldID+`"}`, `"sqs:SendMessage"`, "recreated", "")
	requireEqual(t, queuePolicyOf(t, root, u), shown, "Policy after the IAM user is recreated")
	requireEqual(t, inspectedPolicy(t, s.srv, s.rootCreds, "recreated"), shown,
		"inspected Policy after the IAM user is recreated")

	// Setting the policy again binds it to the new user.
	requireNoErr(t, setPolicy(t, root, u, doc), "grant the new user")
	send(t, sqsClientWith(s.srv, newKey), u, "granted again")
}

func (s *identityStack) testMissingPrincipal(t *testing.T) {
	root := s.rootClient()
	u := createQueue(t, root, "missing-principal", nil)
	doc := grant("Allow", `{"AWS":"`+userARN("nobody")+`"}`, `"sqs:SendMessage"`,
		"missing-principal", "")

	// "If the IAM entity doesn't exist, you will receive an error"
	// (Troubleshoot Amazon SQS API errors, InvalidAttributeValue).
	err := setPolicy(t, root, u, doc)
	requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
	if !strings.Contains(err.Error(), "Invalid principal in policy: "+userARN("nobody")) {
		t.Fatalf("message: %v", err)
	}
	if p, ok := queueAttrs(t, root, u, "Policy")["Policy"]; ok {
		t.Fatalf("Policy after the refused set = %q", p)
	}

	err = errOf(root.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName:  aws.String("created-missing-principal"),
		Attributes: map[string]string{"Policy": doc},
	}))
	requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
	requireQueueMissing(t, root, "created-missing-principal")
}

// inspectedPolicy returns the Policy the inspection API lists for the
// queue name.
func inspectedPolicy(t *testing.T, srv *server, creds aws.Credentials, name string) string {
	t.Helper()
	for _, q := range inspectorWith(srv, creds).queues(t) {
		if q.Name == name {
			return q.Attributes["Policy"]
		}
	}
	t.Fatalf("the inspection API lists no queue %s", name)
	return ""
}

func TestStandalonePoliciesAreStoredAsSent(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "standalone-policy", nil)
	// Standalone there is no identity to ask: an ARN no user has is stored
	// and shown as sent.
	doc := grant("Allow", `{"AWS":"`+userARN("nobody")+`"}`, `"sqs:SendMessage"`,
		"standalone-policy", "")
	requireNoErr(t, setPolicy(t, c, u, doc), "set a policy naming any ARN")
	requireEqual(t, queuePolicyOf(t, c, u), doc, "Policy")
}

// orgClient is an Organizations client of the accounts service, signing
// with the root key.
func (s *orgStack) orgClient() *awsorg.Client {
	return awsorg.New(awsorg.Options{
		Region:           region,
		BaseEndpoint:     aws.String(s.accountsURL),
		Credentials:      credentials.StaticCredentialsProvider{Value: s.rootCreds},
		HTTPClient:       &http.Client{Timeout: clientTimeout},
		RetryMaxAttempts: 1,
	})
}

// orgPath returns the organization's ID and the path of an account
// directly under its root.
func (s *orgStack) orgPath(t *testing.T) (string, string) {
	t.Helper()
	oc := s.orgClient()
	org, err := oc.DescribeOrganization(t.Context(), &awsorg.DescribeOrganizationInput{})
	requireNoErr(t, err, "describe the organization")
	roots, err := oc.ListRoots(t.Context(), &awsorg.ListRootsInput{})
	requireNoErr(t, err, "list the roots")
	if len(roots.Roots) != 1 {
		t.Fatalf("roots: %+v", roots.Roots)
	}
	id := aws.ToString(org.Organization.Id)
	return id, id + "/" + aws.ToString(roots.Roots[0].Id) + "/"
}

// createAccount creates another member account and returns its ID.
func (s *orgStack) createAccount(t *testing.T, name string) string {
	t.Helper()
	out, err := s.orgClient().CreateAccount(t.Context(), &awsorg.CreateAccountInput{
		AccountName: aws.String(name), Email: aws.String(name + "@example.com"),
	})
	requireNoErr(t, err, "create account "+name)
	st := out.CreateAccountStatus
	if st == nil || st.State != orgtypes.CreateAccountStateSucceeded {
		t.Fatalf("create account %s: %+v", name, st)
	}
	return aws.ToString(st.AccountId)
}

// accountUser creates a user of account, through its organization role,
// allowed sqs:SendMessage by its identity policy, and returns an SQS client
// signing with its key.
func (s *orgStack) accountUser(t *testing.T, account, name string) *sqs.Client {
	t.Helper()
	ic := iamClient(s.identityURL, assumeOrganizationRole(t, s.identityURL, account))
	key := createUserKey(t, ic, name)
	putUserPolicy(t, ic, name, allowPolicy(`"sqs:SendMessage"`, "*"))
	return sqsClientWith(s.srv, key)
}

// orgKeyStaleness is how long identity keeps an account's organization
// keys (its README, "Staleness"), with a margin.
const orgKeyStaleness = 40 * time.Second

func TestOrganizationKeys(t *testing.T) {
	s := startOrgStack(t)
	root := s.rootClient()
	orgID, path := s.orgPath(t)

	byID := createQueue(t, root, "org-id", nil)
	requireNoErr(t, setPolicy(t, root, byID, grant("Allow", `"*"`, `"sqs:SendMessage"`,
		"org-id", `{"StringEquals":{"aws:PrincipalOrgID":"`+orgID+`"}}`)),
		"grant the organization by ID")
	byPath := createQueue(t, root, "org-path", nil)
	requireNoErr(t, setPolicy(t, root, byPath, grant("Allow", `"*"`, `"sqs:SendMessage"`,
		"org-path", `{"ForAnyValue:StringLike":{"aws:PrincipalOrgPaths":"`+path+`*"}}`)),
		"grant the organization by path")

	member := s.memberUser(t, "org-member", allowPolicy(`"sqs:SendMessage"`, "*"))
	send(t, member, byID, "a member, by organization ID")
	send(t, member, byPath, "a member, by organization path")
	session := sqsClientWith(s.srv, s.memberSession)
	send(t, session, byID, "a member's role session, by organization ID")
	send(t, session, byPath, "a member's role session, by organization path")

	// An account removed from the organization is outside it: its user,
	// allowed before, is refused once identity's keys for it are stale.
	leaving := s.createAccount(t, "Leaving")
	gone := s.accountUser(t, leaving, "leaving")
	goneARN := "arn:aws:iam::" + leaving + ":user/leaving"
	send(t, gone, byID, "before the account leaves")
	_, err := s.orgClient().RemoveAccountFromOrganization(t.Context(),
		&awsorg.RemoveAccountFromOrganizationInput{AccountId: aws.String(leaving)})
	requireNoErr(t, err, "remove the account from the organization")

	deadline := time.Now().Add(orgKeyStaleness)
	for {
		err = errOf(gone.SendMessage(t.Context(), sendTo(byID)))
		if err != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	requireResourceDenied(t, err, goneARN, "sqs:SendMessage", queueARN("org-id"),
		noResourceAllow)
	requireResourceDenied(t, errOf(gone.SendMessage(t.Context(), sendTo(byPath))), goneARN,
		"sqs:SendMessage", queueARN("org-path"), noResourceAllow)

	// The member keeps its grant.
	send(t, member, byID, "a member, after another account left")
}
