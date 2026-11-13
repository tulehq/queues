//go:build e2e

package e2e

import (
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsorg "github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// A role session: accounts and identity are built from their checkouts and
// run as processes with the root key queues has; identity asks accounts
// about member accounts for AssumeRole, and queues asks identity about
// every key it does not hold. The root user assumes
// OrganizationAccountAccessRole in a member account, and the session's
// credentials call queues.

// accountsDirEnv names the accounts checkout to build, for a layout where it
// is not queues' sibling.
const accountsDirEnv = "TULE_E2E_ACCOUNTS_DIR"

// organizationRole is the role identity lets the root user assume in a
// member account.
const organizationRole = "OrganizationAccountAccessRole"

func TestIdentityRoleSession(t *testing.T) {
	// The processes start before the app: startServer sets TULE_ variables
	// in this process, and envWithoutTule keeps them out of the children.
	accountsPort := identityFreePort(t)
	accountsDir := t.TempDir()
	startProcess(
		t,
		"accounts",
		buildService(t, "accounts", accountsDirEnv),
		accountsDir,
		accountsPort,
		[]string{
			"TULE_Server_Port=" + strconv.Itoa(accountsPort),
			"TULE_Store_Path=" + filepath.Join(accountsDir, "accounts.db"),
		},
	)
	accountsURL := "http://127.0.0.1:" + strconv.Itoa(accountsPort)
	member := createMemberAccount(t, accountsURL)

	identityPort := identityFreePort(t)
	identityURL := "http://127.0.0.1:" + strconv.Itoa(identityPort)
	startIdentity(t, identityPort, "TULE_Identity_AccountsEndpoint="+accountsURL)

	srv := startServer(t, map[string]string{
		"TULE_Auth_AccessKeyID":      identityRootKeyID,
		"TULE_Auth_SecretAccessKey":  identityRootSecret,
		"TULE_Auth_IdentityEndpoint": identityURL,
	})
	session := assumeOrganizationRole(t, identityURL, member)

	t.Run("the member account's session is refused across accounts", func(t *testing.T) {
		// The session verifies, and its role's AdministratorAccess allows
		// everything, but it is the member account's and the queues are the
		// management account's: AWS needs a resource-based policy to grant
		// a cross-account request, and no queue policy grants this one
		// (CreateQueue is never shared across accounts). TestQueuePolicies
		// has the requests a queue policy grants.
		requireMatch(t, regexp.MustCompile(`^ASIA`), session.AccessKeyID, "session key ID")
		sessionARN := "arn:aws:sts::" + member + ":assumed-role/" + organizationRole + "/e2e"
		c := sqsClientWith(srv, session)
		err := errOf(c.CreateQueue(t.Context(),
			&sqs.CreateQueueInput{QueueName: aws.String("from-session")}))
		requireCrossAccountDenied(t, err, sessionARN, "sqs:CreateQueue", queueARN("from-session"))
		requireQueueMissing(t, sqsClientWith(srv, aws.Credentials{
			AccessKeyID: identityRootKeyID, SecretAccessKey: identityRootSecret,
		}), "from-session")
		requireAPIErrorBody(
			t,
			inspectorWith(srv, session).do(t, http.MethodGet, "/_queues/queues", nil),
			http.StatusForbidden,
			"AccessDenied",
			"inspection with the member session",
		)
	})

	t.Run("a tampered token is refused", func(t *testing.T) {
		tampered := session
		tampered.SessionToken = tamper(session.SessionToken)
		requireSessionRefused(t, srv, tampered)
	})

	t.Run("the key without its token is refused", func(t *testing.T) {
		tokenless := session
		tokenless.SessionToken = ""
		requireSessionRefused(t, srv, tokenless)
	})
}

// createMemberAccount creates the organization in accounts, with the root
// key, and a member account, and returns the member's ID.
func createMemberAccount(t *testing.T, accountsURL string) string {
	t.Helper()
	oc := awsorg.New(awsorg.Options{
		Region:       region,
		BaseEndpoint: aws.String(accountsURL),
		Credentials: credentials.StaticCredentialsProvider{Value: aws.Credentials{
			AccessKeyID: identityRootKeyID, SecretAccessKey: identityRootSecret,
		}},
		HTTPClient:       &http.Client{Timeout: clientTimeout},
		RetryMaxAttempts: 1,
	})
	_, err := oc.CreateOrganization(t.Context(), &awsorg.CreateOrganizationInput{})
	requireNoErr(t, err, "create organization")
	out, err := oc.CreateAccount(t.Context(), &awsorg.CreateAccountInput{
		AccountName: aws.String("Member"),
		Email:       aws.String("member@example.com"),
	})
	requireNoErr(t, err, "create member account")
	st := out.CreateAccountStatus
	if st == nil || st.State != orgtypes.CreateAccountStateSucceeded {
		t.Fatalf("create member account: %+v", st)
	}
	return aws.ToString(st.AccountId)
}

// assumeOrganizationRole assumes the organization role in member through
// identity, as the root user, and returns the session's credentials.
func assumeOrganizationRole(t *testing.T, identityURL, member string) aws.Credentials {
	t.Helper()
	sc := sts.New(sts.Options{
		Region:       region,
		BaseEndpoint: aws.String(identityURL),
		Credentials: credentials.StaticCredentialsProvider{Value: aws.Credentials{
			AccessKeyID: identityRootKeyID, SecretAccessKey: identityRootSecret,
		}},
		HTTPClient:       &http.Client{Timeout: clientTimeout},
		RetryMaxAttempts: 1,
	})
	out, err := sc.AssumeRole(t.Context(), &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:aws:iam::" + member + ":role/" + organizationRole),
		RoleSessionName: aws.String("e2e"),
	})
	requireNoErr(t, err, "assume role in "+member)
	return aws.Credentials{
		AccessKeyID:     aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(out.Credentials.SessionToken),
	}
}

// requireSessionRefused fails unless both APIs refuse creds with 403
// InvalidClientTokenId, SQS's answer to a security token that is not valid
// (Common Errors: the access key ID "does not exist in our records"), and
// the refused call created nothing.
func requireSessionRefused(t *testing.T, srv *server, creds aws.Credentials) {
	t.Helper()
	err := errOf(sqsClientWith(srv, creds).CreateQueue(t.Context(),
		&sqs.CreateQueueInput{QueueName: aws.String("refused-session")}))
	requireCode(t, err, "InvalidClientTokenId")
	requireStatus(t, err, http.StatusForbidden)
	requireQueueMissing(t, sqsClientWith(srv, aws.Credentials{
		AccessKeyID: identityRootKeyID, SecretAccessKey: identityRootSecret,
	}), "refused-session")

	requireAPIErrorBody(t, inspectorWith(srv, creds).do(t, http.MethodGet, "/_queues/queues", nil),
		http.StatusForbidden, "InvalidClientTokenId", "inspection with refused session credentials")
}

// tamper changes one character of a session token, keeping its length and
// alphabet.
func tamper(token string) string {
	if token == "" {
		return "x"
	}
	b := []byte(token)
	i := len(b) / 2
	if b[i] == 'A' {
		b[i] = 'B'
	} else {
		b[i] = 'A'
	}
	return string(b)
}
