package app

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"tule.dev/sigv4"
)

// policyUserARN is the principal policyIdentity hands out for any key but
// the root key.
const policyUserARN = "arn:aws:iam::000000000000:user/dev"

// policyIdentity stands in for identity's signing-key endpoint at version
// 2: every key is a user of the default account with secret "test" and the
// given identity policies.
func policyIdentity(t *testing.T, policies ...string) string {
	t.Helper()
	return policyIdentityIn(t, "000000000000", policyUserARN, policies...)
}

// policyIdentityIn is policyIdentity for the principal arn of account.
func policyIdentityIn(t *testing.T, account, arn string, policies ...string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			AccessKeyID string `json:"AccessKeyId"`
			Date        string
			Region      string
			Service     string
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		docs := make([]json.RawMessage, 0, len(policies))
		for _, p := range policies {
			docs = append(docs, json.RawMessage(p))
		}
		key := sigv4.DeriveSigningKey("test", req.Date, req.Region, req.Service)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Principal": map[string]any{
				"AccountId": account, "Arn": arn,
				"AccessKeyId": req.AccessKeyID, "Policies": docs,
			},
			"SigningKey": base64.StdEncoding.EncodeToString(key),
			"Version":    2,
		})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func allowDoc(action, resource string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"` + action +
		`","Resource":"` + resource + `"}]}`
}

const ordersARN = "arn:aws:sqs:us-east-1:000000000000:orders"

func deniedMessage(action, resource string) string {
	return "User: " + policyUserARN + " is not authorized to perform: " + action +
		" on resource: " + resource + " because no identity-based policy allows the " +
		action + " action"
}

// requireDenied fails unless err is SQS's 403 AccessDenied with message.
func requireDenied(t *testing.T, err error, message string) {
	t.Helper()
	requireAPIError(t, err, "AccessDenied", http.StatusForbidden)
	var apiErr smithy.APIError
	if errors.As(err, &apiErr); apiErr.ErrorMessage() != message {
		t.Fatalf("message:\n got %s\nwant %s", apiErr.ErrorMessage(), message)
	}
}

func TestAUserWithoutAPolicyIsDenied(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t))
	c, base := startApp(t, t.TempDir(), nil)

	_, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{}, withOtherKey)
	requireDenied(t, err, deniedMessage("sqs:listqueues", "*"))

	// A queue that does not exist is denied too: the resource is the ARN
	// the request names.
	_, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(base + "/000000000000/orders"), MessageBody: aws.String("x"),
	}, withOtherKey)
	requireDenied(t, err, deniedMessage("sqs:sendmessage", ordersARN))

	// The root key is never evaluated.
	if _, err = c.ListQueues(t.Context(), &sqs.ListQueuesInput{}); err != nil {
		t.Fatalf("list queues with the root key: %v", err)
	}
}

func TestAPolicyAllowsItsActionOnItsResource(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t,
		allowDoc("sqs:SendMessage", ordersARN)))
	c, _ := startApp(t, t.TempDir(), nil)
	orders := createQueue(t, c, "orders", nil)
	other := createQueue(t, c, "other", nil)

	if _, err := c.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(orders),
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("a"), MessageBody: aws.String("one")},
			{Id: aws.String("b"), MessageBody: aws.String("two")},
		},
	}, withOtherKey); err != nil {
		t.Fatalf("SendMessageBatch, evaluated as sqs:SendMessage: %v", err)
	}
	_, err := c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(other), MessageBody: aws.String("x"),
	}, withOtherKey)
	requireDenied(t, err, deniedMessage("sqs:sendmessage",
		"arn:aws:sqs:us-east-1:000000000000:other"))
	_, err = c.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(orders),
	}, withOtherKey)
	requireDenied(t, err, deniedMessage("sqs:receivemessage", ordersARN))
}

func TestCreatingATaggedQueueNeedsTagQueue(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t,
		allowDoc("sqs:CreateQueue", "*")))
	c, _ := startApp(t, t.TempDir(), nil)

	if _, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("plain"),
	}, withOtherKey); err != nil {
		t.Fatalf("create an untagged queue: %v", err)
	}
	_, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("orders"), Tags: map[string]string{"team": "blue"},
	}, withOtherKey)
	requireDenied(t, err, deniedMessage("sqs:tagqueue", ordersARN))
}

func TestAResourceTagConditionReadsTheQueuesTags(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:SendMessage",
		"Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/team":"blue"}}}]}`
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, doc))
	c, _ := startApp(t, t.TempDir(), nil)
	blue, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("blue"), Tags: map[string]string{"team": "blue"},
	})
	if err != nil {
		t.Fatalf("create blue: %v", err)
	}
	red := createQueue(t, c, "red", nil)

	if _, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: blue.QueueUrl, MessageBody: aws.String("x"),
	}, withOtherKey); err != nil {
		t.Fatalf("send to the tagged queue: %v", err)
	}
	_, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(red), MessageBody: aws.String("x"),
	}, withOtherKey)
	requireDenied(t, err, deniedMessage("sqs:sendmessage",
		"arn:aws:sqs:us-east-1:000000000000:red"))
}

func TestTooManyTagKeysAreRefusedBeforeEvaluation(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("sqs:*", "*")))
	c, _ := startApp(t, t.TempDir(), nil)
	u := createQueue(t, c, "orders", nil)
	keys := make([]string, 51)
	for i := range keys {
		keys[i] = "k" + strings.Repeat("x", i)
	}
	_, err := c.UntagQueue(t.Context(), &sqs.UntagQueueInput{
		QueueUrl: aws.String(u), TagKeys: keys,
	}, withOtherKey)
	requireAPIError(t, err, "InvalidParameterValue", http.StatusBadRequest)
}

// signedPost sends a request signed with the other key, the one
// policyIdentity knows.
func signedPost(t *testing.T, method, url, contentType, body string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	sum := sha256.Sum256([]byte(body))
	if err = v4.NewSigner().SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: "AKIDOTHER", SecretAccessKey: "test"}, req,
		hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp.StatusCode, resp.Header, string(got)
}

func TestAQueryDenialIsAnXMLAccessDenied(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t))
	_, base := startApp(t, t.TempDir(), nil)

	status, _, body := signedPost(t, http.MethodPost, base+"/000000000000/orders",
		"application/x-www-form-urlencoded", "Action=PurgeQueue&Version=2012-11-05")
	var doc struct {
		Error struct{ Type, Code, Message string }
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if status != http.StatusForbidden || doc.Error.Type != "Sender" ||
		doc.Error.Code != "AccessDenied" ||
		doc.Error.Message != deniedMessage("sqs:purgequeue", ordersARN) {
		t.Fatalf("status %d, error %+v", status, doc.Error)
	}
}

func TestAJSONDenialCarriesTheQueryCode(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t))
	_, base := startApp(t, t.TempDir(), nil)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/",
		strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
	sum := sha256.Sum256([]byte(`{}`))
	if err = v4.NewSigner().SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: "AKIDOTHER", SecretAccessKey: "test"}, req,
		hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var doc struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden ||
		resp.Header.Get("X-Amzn-Query-Error") != "AccessDenied;Sender" ||
		doc.Type != "AccessDenied" || doc.Message != deniedMessage("sqs:listqueues", "*") {
		t.Fatalf("status %d, query error %q, body %+v", resp.StatusCode,
			resp.Header.Get("X-Amzn-Query-Error"), doc)
	}
}

func TestInspectionNeedsTuleInspect(t *testing.T) {
	t.Run("denied", func(t *testing.T) {
		t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("sqs:*", "*")))
		_, base := startApp(t, t.TempDir(), nil)
		status, header, body := signedPost(t, http.MethodGet, base+"/_queues/queues", "", "")
		want := deniedMessage("tule:inspect", "arn:aws:sqs:us-east-1:000000000000:*")
		if status != http.StatusForbidden || header.Get("X-Amzn-Errortype") != "AccessDenied" ||
			!strings.Contains(body, want) {
			t.Fatalf("status %d, type %q, body %s", status, header.Get("X-Amzn-Errortype"), body)
		}
	})
	t.Run("allowed", func(t *testing.T) {
		t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t,
			allowDoc("tule:Inspect", "arn:aws:sqs:us-east-1:000000000000:*")))
		_, base := startApp(t, t.TempDir(), nil)
		if status, _, body := signedPost(
			t,
			http.MethodGet,
			base+"/_queues/queues",
			"",
			"",
		); status != http.StatusOK {
			t.Fatalf("status %d, body %s", status, body)
		}
	})
}

func TestAnotherAccountsPrincipalNeedsAResourcePolicy(t *testing.T) {
	const member = "arn:aws:sts::444455556666:assumed-role/OrganizationAccountAccessRole/e2e"
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentityIn(t, "444455556666", member,
		allowDoc("*", "*")))
	_, base := startApp(t, t.TempDir(), nil)

	want := "User: " + member + " is not authorized to perform: sqs:purgequeue on resource: " +
		ordersARN + " because no resource-based policy allows the sqs:purgequeue action"
	status, _, body := signedPost(t, http.MethodPost, base+"/000000000000/orders",
		"application/x-www-form-urlencoded", "Action=PurgeQueue&Version=2012-11-05")
	if status != http.StatusForbidden || !strings.Contains(body, "<Code>AccessDenied</Code>") ||
		!strings.Contains(body, "<Message>"+want+"</Message>") {
		t.Fatalf("Query: status %d, body %s", status, body)
	}

	status, header, body := signedPost(t, http.MethodGet, base+"/_queues/queues", "", "")
	if status != http.StatusForbidden || header.Get("X-Amzn-Errortype") != "AccessDenied" ||
		!strings.Contains(body, "because no resource-based policy allows the tule:inspect action") {
		t.Fatalf("inspection: status %d, body %s", status, body)
	}
}

func TestAResourceTagDenyWithAnEscapedKeyApplies(t *testing.T) {
	// The key is aws:ResourceTag/team with its T escaped in JSON, which the
	// policy parser reads as the plain key.
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"sqs:SendMessage",
		"Resource":"*","Condition":{"StringEquals":{"aws:Resource\u0054ag/team":"red"}}}]}`
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("sqs:*", "*"), deny))
	c, _ := startApp(t, t.TempDir(), nil)
	red, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("red"), Tags: map[string]string{"team": "red"},
	})
	if err != nil {
		t.Fatalf("create red: %v", err)
	}

	_, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: red.QueueUrl, MessageBody: aws.String("x"),
	}, withOtherKey)
	requireDenied(t, err, "User: "+policyUserARN+" is not authorized to perform: "+
		"sqs:sendmessage on resource: arn:aws:sqs:us-east-1:000000000000:red with an "+
		"explicit deny in an identity-based policy")
}

// signedJSON sends a JSON request for op signed with the other key, the
// one policyIdentity knows, and returns its status and body.
func signedJSON(t *testing.T, base, op, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/",
		strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
	sum := sha256.Sum256([]byte(body))
	if err = v4.NewSigner().SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: "AKIDOTHER", SecretAccessKey: "test"}, req,
		hex.EncodeToString(sum[:]), "sqs", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp.StatusCode, string(got)
}

func TestARepeatedTagKeyIsRefusedBeforeEvaluation(t *testing.T) {
	// The policy allows team=blue only. With the later value winning, the
	// request would be evaluated, and stored, as one or the other; it is
	// refused instead, before evaluation.
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*",
		"Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/team":"blue"}}}]}`
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, doc))
	c, base := startApp(t, t.TempDir(), nil)
	u := createQueue(t, c, "orders", nil)

	for name, body := range map[string]string{
		"TagQueue":    `{"QueueUrl":"` + u + `","Tags":{"team":"blue","team":"red"}}`,
		"CreateQueue": `{"QueueName":"tagged","tags":{"team":"red","team":"blue"}}`,
	} {
		status, got := signedJSON(t, base, name, body)
		if status != http.StatusBadRequest || !strings.Contains(got, "InvalidParameterValue") ||
			!strings.Contains(got, "The tag key is repeated") {
			t.Fatalf("%s: status %d, body %s", name, status, got)
		}
	}
	tags, err := c.ListQueueTags(t.Context(), &sqs.ListQueueTagsInput{QueueUrl: aws.String(u)})
	if err != nil || len(tags.Tags) != 0 {
		t.Fatalf("tags after the refusal = %v, %v", tags, err)
	}
}

func TestAnotherAccountsQueueIsAnsweredBeforeEvaluation(t *testing.T) {
	// A user with no policy would be denied anything; a queue of another
	// account is answered as not existing first, as the operation answers
	// it, and never evaluated on an ARN the client wrote.
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t))
	c, base := startApp(t, t.TempDir(), nil)
	createQueue(t, c, "orders", nil)
	const missing = "AWS.SimpleQueueService.NonExistentQueue"

	_, err := c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(base + "/111122223333/orders"), MessageBody: aws.String("x"),
	}, withOtherKey)
	requireAPIError(t, err, missing, http.StatusBadRequest)

	_, err = c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{
		QueueName: aws.String("orders"), QueueOwnerAWSAccountId: aws.String("111122223333"),
	}, withOtherKey)
	requireAPIError(t, err, missing, http.StatusBadRequest)

	status, _, body := signedPost(t, http.MethodPost, base+"/111122223333/orders",
		"application/x-www-form-urlencoded", "Action=PurgeQueue&Version=2012-11-05")
	if status != http.StatusBadRequest || !strings.Contains(body, missing) {
		t.Fatalf("Query to another account's queue path: status %d, body %s", status, body)
	}
}

func TestTheStaticKeyIsRootOfAConfiguredAccount(t *testing.T) {
	// The static key's principal carries the configured account, so it is
	// that account's root user and never evaluated, standalone as with
	// identity.
	t.Setenv("TULE_Queues_AccountID", "123456789012")
	c, base := startApp(t, t.TempDir(), nil)
	u := createQueue(t, c, "orders", nil)
	if want := base + "/123456789012/orders"; u != want {
		t.Fatalf("queue URL = %s, want %s", u, want)
	}
	if _, err := c.DeleteQueue(
		t.Context(),
		&sqs.DeleteQueueInput{QueueUrl: aws.String(u)},
	); err != nil {
		t.Fatalf("delete with the static key: %v", err)
	}
}

func TestTheRequestedRegionIsTheServicesNotTheSigners(t *testing.T) {
	// Any signed region verifies here, so a region the caller signs for
	// must not switch off a Deny on the region the queues are in.
	deny := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"sqs:*",
		"Resource":"*","Condition":{"StringEquals":{"aws:RequestedRegion":"us-east-1"}}}]}`
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("sqs:*", "*"), deny))
	c, _ := startApp(t, t.TempDir(), nil)

	_, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{}, withOtherKey,
		func(o *sqs.Options) { o.Region = "eu-west-1" })
	requireDenied(t, err, "User: "+policyUserARN+" is not authorized to perform: "+
		"sqs:listqueues on resource: * with an explicit deny in an identity-based policy")
}

func TestTagKeysDifferingOnlyByCaseAreRefusedForEvaluatedPrincipals(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("sqs:*", "*")))
	c, base := startApp(t, t.TempDir(), nil)
	u := createQueue(t, c, "orders", nil)
	refused := func(name string, status int, body string) {
		t.Helper()
		if status != http.StatusBadRequest ||
			!strings.Contains(body, "differs only by case from another tag key") {
			t.Fatalf("%s: status %d, body %s", name, status, body)
		}
	}

	status, body := signedJSON(t, base, "TagQueue",
		`{"QueueUrl":"`+u+`","Tags":{"env":"dev","ENV":"prod"}}`)
	refused("TagQueue with env and ENV", status, body)
	status, body = signedJSON(t, base, "CreateQueue",
		`{"QueueName":"cased","tags":{"Env":"dev","env":"prod"}}`)
	refused("CreateQueue with Env and env", status, body)

	// A stored key is compared too.
	if _, err := c.TagQueue(t.Context(), &sqs.TagQueueInput{
		QueueUrl: aws.String(u), Tags: map[string]string{"env": "dev"},
	}); err != nil {
		t.Fatalf("tag with the root key: %v", err)
	}
	status, body = signedJSON(t, base, "TagQueue", `{"QueueUrl":"`+u+`","Tags":{"ENV":"prod"}}`)
	refused("TagQueue adding ENV beside env", status, body)
	if status, body = signedJSON(t, base, "TagQueue",
		`{"QueueUrl":"`+u+`","Tags":{"env":"prod"}}`); status != http.StatusOK {
		t.Fatalf("TagQueue overwriting env: status %d, body %s", status, body)
	}
	tags, err := c.ListQueueTags(t.Context(), &sqs.ListQueueTagsInput{QueueUrl: aws.String(u)})
	if err != nil || len(tags.Tags) != 1 || tags.Tags["env"] != "prod" {
		t.Fatalf("tags = %v, %v", tags, err)
	}
}

func TestAQueueHoldingCaseCollidingTagsIsNotEvaluated(t *testing.T) {
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, allowDoc("sqs:*", "*")))
	c, _ := startApp(t, t.TempDir(), nil)
	u := createQueue(t, c, "orders", nil)
	// The root user's writes are unchanged, so it can store both.
	if _, err := c.TagQueue(t.Context(), &sqs.TagQueueInput{
		QueueUrl: aws.String(u), Tags: map[string]string{"env": "dev", "ENV": "prod"},
	}); err != nil {
		t.Fatalf("tag with the root key: %v", err)
	}

	_, err := c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("x"),
	}, withOtherKey)
	requireAPIError(t, err, "InternalFailure", http.StatusInternalServerError)
	if _, err = c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("x"),
	}); err != nil {
		t.Fatalf("send with the root key: %v", err)
	}
}

func TestALongQueueNameIsRefusedBeforeEvaluation(t *testing.T) {
	// Matching costs about the pattern's length times the resource's, so a
	// queue name past SQS's 80 characters never reaches the evaluation: a
	// user with no policy, who would be denied anything, gets the
	// validation answer.
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t))
	c, base := startApp(t, t.TempDir(), nil)
	long := strings.Repeat("q", 81)
	huge := strings.Repeat("q", 1<<20)

	for _, name := range []string{long, huge} {
		_, err := c.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: aws.String(base + "/000000000000/" + name), MessageBody: aws.String("x"),
		}, withOtherKey)
		requireAPIError(t, err, "AWS.SimpleQueueService.NonExistentQueue", http.StatusBadRequest)
	}
	_, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(long)},
		withOtherKey)
	requireAPIError(t, err, "InvalidParameterValue", http.StatusBadRequest)
	_, err = c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(long)},
		withOtherKey)
	requireAPIError(t, err, "InvalidParameterValue", http.StatusBadRequest)
	status, _, body := signedPost(t, http.MethodPost, base+"/000000000000/"+long,
		"application/x-www-form-urlencoded", "Action=PurgeQueue&Version=2012-11-05")
	if status != http.StatusBadRequest || !strings.Contains(body, "InvalidParameterValue") {
		t.Fatalf("Query to a long queue path: status %d, body %s", status, body)
	}
}

func TestAQueryIndexAliasIsRefusedBeforeEvaluation(t *testing.T) {
	// Tag.1 and Tag.01 would be one entry, its Value whichever spelling the
	// map gives last: allowed as env=dev, stored as env=prod.
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*",
		"Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/env":"dev"}}}]}`
	t.Setenv("TULE_Auth_IdentityEndpoint", policyIdentity(t, doc))
	c, base := startApp(t, t.TempDir(), nil)
	u := createQueue(t, c, "orders", nil)

	for name, form := range map[string]string{
		u:          "Action=TagQueue&Version=2012-11-05",
		base + "/": "Action=CreateQueue&Version=2012-11-05&QueueName=aliased",
	} {
		status, _, body := signedPost(t, http.MethodPost, name,
			"application/x-www-form-urlencoded",
			form+"&Tag.1.Key=env&Tag.1.Value=dev&Tag.01.Value=prod")
		if status != http.StatusNotFound || !strings.Contains(body, "MalformedQueryString") {
			t.Fatalf("%s: status %d, body %s", form, status, body)
		}
	}
	tags, err := c.ListQueueTags(t.Context(), &sqs.ListQueueTagsInput{QueueUrl: aws.String(u)})
	if err != nil || len(tags.Tags) != 0 {
		t.Fatalf("tags = %v, %v", tags, err)
	}
	_, err = c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("aliased")})
	requireAPIError(t, err, "AWS.SimpleQueueService.NonExistentQueue", http.StatusBadRequest)
}

func TestARepeatedJSONMemberIsEvaluatedAsItIsActedOn(t *testing.T) {
	// encoding/json keeps the last of a repeated member, and matches member
	// names without regard to case. The request is decoded once, so the
	// queue evaluated is the queue acted on, whichever spelling wins.
	t.Setenv(
		"TULE_Auth_IdentityEndpoint",
		policyIdentity(t, allowDoc("sqs:SendMessage", ordersARN)),
	)
	c, base := startApp(t, t.TempDir(), nil)
	orders := createQueue(t, c, "orders", nil)
	other := createQueue(t, c, "other", nil)
	otherARN := "arn:aws:sqs:us-east-1:000000000000:other"

	for name, body := range map[string]string{
		"repeated":   `{"QueueUrl":"` + orders + `","QueueUrl":"` + other + `","MessageBody":"x"}`,
		"other case": `{"QueueUrl":"` + orders + `","queueurl":"` + other + `","MessageBody":"x"}`,
	} {
		status, got := signedJSON(t, base, "SendMessage", body)
		if status != http.StatusForbidden ||
			!strings.Contains(got, deniedMessage("sqs:sendmessage", otherARN)) {
			t.Fatalf("%s: status %d, body %s", name, status, got)
		}
	}
	counts, err := c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(
			other,
		),
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages"},
	})
	if err != nil || counts.Attributes["ApproximateNumberOfMessages"] != "0" {
		t.Fatalf("the other queue's messages = %v, %v", counts, err)
	}
	if status, got := signedJSON(
		t,
		base,
		"SendMessage",
		`{"QueueUrl":"`+other+`","QueueUrl":"`+orders+`","MessageBody":"x"}`,
	); status != http.StatusOK {
		t.Fatalf("repeated, the allowed queue last: status %d, body %s", status, got)
	}
}
