package queues

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gasmod/gas"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
	"tule.dev/queues/servicesend"
)

const (
	s3Service    = "s3.amazonaws.com"
	ordersARN    = "arn:aws:sqs:us-east-1:000000000000:orders"
	photosBucket = "arn:aws:s3:::abc"
)

func TestTheServiceRequestFieldsAreChecked(t *testing.T) {
	valid := servicesend.Request{
		QueueArn: ordersARN, ServicePrincipal: s3Service,
		SourceArn: photosBucket, SourceAccount: "000000000000",
	}
	if r := checkServiceRequest(&valid); r != nil {
		t.Fatalf("a valid request: %v", r)
	}
	for what, change := range map[string]func(*servicesend.Request){
		"no service principal":        func(r *servicesend.Request) { r.ServicePrincipal = "" },
		"a service principal ARN":     func(r *servicesend.Request) { r.ServicePrincipal = photosBucket },
		"a non-AWS service principal": func(r *servicesend.Request) { r.ServicePrincipal = "s3.example.com" },
		"a SourceArn that is no ARN":  func(r *servicesend.Request) { r.SourceArn = "photos" },
		"a SourceArn over the cap": func(r *servicesend.Request) {
			r.SourceArn = photosBucket + strings.Repeat("x", authz.MaxValueLength)
		},
		"a short SourceAccount":   func(r *servicesend.Request) { r.SourceAccount = "12" },
		"a SourceAccount of text": func(r *servicesend.Request) { r.SourceAccount = "00000000000a" },
	} {
		req := valid
		change(&req)
		r := checkServiceRequest(&req)
		if r == nil || r.status != http.StatusBadRequest ||
			r.code != servicesend.CodeInvalidRequest {
			t.Errorf("%s: %v, want 400 InvalidRequest", what, r)
		}
	}
}

func TestTheMessageBodyIsDecodedAsReceived(t *testing.T) {
	for raw, want := range map[string]struct {
		body string
		lone bool
	}{
		``:                       {},
		`null`:                   {},
		`"<a & b>"`:              {body: "<a & b>"},
		`"\u003ca\u0026b\u003e"`: {body: "<a&b>"},
		`"\ud83d\ude00"`:         {body: "\U0001F600"},
		`"\\ud800"`:              {body: `\ud800`},
		`"\ud800"`:               {body: "\uFFFD", lone: true},
		`"\udc00"`:               {body: "\uFFFD", lone: true},
		`"\ud800x"`:              {body: "\uFFFDx", lone: true},
		`"\ud800\u0041"`:         {body: "\uFFFDA", lone: true},
		`"\ud800\ud800"`:         {body: "\uFFFD\uFFFD", lone: true},
		`"\ud83d\ude00\ud800"`:   {body: "\U0001F600\uFFFD", lone: true},
		`"a\"\ud83d\ude00\\"`:    {body: "a\"\U0001F600\\"},
	} {
		body, lone, r := messageBody(json.RawMessage(raw))
		if r != nil || body != want.body || lone != want.lone {
			t.Errorf("%s: %q, %t, %v; want %q, %t", raw, body, lone, r, want.body, want.lone)
		}
	}
	for _, raw := range []string{`1`, `{}`, `["x"]`, `true`} {
		if _, _, r := messageBody(json.RawMessage(raw)); r == nil ||
			r.code != servicesend.CodeInvalidRequest {
			t.Errorf("%s: %v, want InvalidRequest", raw, r)
		}
	}
}

func TestTheQueueArnNamesAQueueOfTheService(t *testing.T) {
	m := NewModule(&contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"},
		nil, nil)(nil, nil)
	if name, r := m.serviceQueue(ordersARN); r != nil || name != "orders" {
		t.Fatalf("the service's queue: %q, %v", name, r)
	}
	for arn, status := range map[string]int{
		"arn:aws:sqs:us-east-1:111111111111:orders":      http.StatusNotFound,
		"arn:aws:sqs:eu-west-1:000000000000:orders":      http.StatusNotFound,
		"arn:aws-cn:sqs:us-east-1:000000000000:orders":   http.StatusNotFound,
		"arn:aws:sqs:us-east-1:000000000000:bad.name":    http.StatusNotFound,
		"arn:aws:sqs:us-east-1:000000000000:a:b":         http.StatusNotFound,
		"arn:aws:sns:us-east-1:000000000000:orders":      http.StatusBadRequest,
		"arn:aws:sqs:us-east-1:000000000000:":            http.StatusBadRequest,
		"orders":                                         http.StatusBadRequest,
		"":                                               http.StatusBadRequest,
		"https://sqs.us-east-1.amazonaws.com/0/orders":   http.StatusBadRequest,
		"arn:aws:sqs:us-east-1:000000000000:orders.fifo": 0,
	} {
		_, r := m.serviceQueue(arn)
		switch {
		case status == 0 && r != nil:
			t.Errorf("%s: %v, want a queue", arn, r)
		case status != 0 && (r == nil || r.status != status):
			t.Errorf("%s: %v, want %d", arn, r, status)
		}
	}
}

// sendStore is a store with one queue, orders, whose current ID is
// current while QueueAccess reports evaluated, as when the queue was
// deleted and created again between the two reads. It records the sends.
// Only the methods the endpoint calls are implemented.
type sendStore struct {
	contracts.QueueProvider
	policy    string
	sent      []contracts.SendInput
	evaluated contracts.QueueID
	current   contracts.QueueID
}

func (s *sendStore) QueueAccess(context.Context, string) (*contracts.QueueAccess, error) {
	return &contracts.QueueAccess{ID: s.evaluated, Policy: s.policy}, nil
}

func (s *sendStore) pinned(ctx context.Context, name string) bool {
	id, ok := contracts.PinnedQueue(ctx, name)
	return ok && id == s.current
}

func (s *sendStore) QueueInfo(ctx context.Context, name string) (*contracts.Queue, error) {
	if !s.pinned(ctx, name) {
		return nil, contracts.ErrQueueNotFound
	}
	return &contracts.Queue{Name: name, Attributes: map[string]string{
		contracts.AttrDelaySeconds: "0", contracts.AttrMessageRetentionPeriod: "345600",
		contracts.AttrReceiveMessageWaitTime: "0", contracts.AttrVisibilityTimeout: "30",
		contracts.AttrMaximumMessageSize: "1048576",
	}}, nil
}

func (s *sendStore) SendMessages(
	ctx context.Context, name string, msgs []contracts.SendInput,
) ([]contracts.SendResult, error) {
	if !s.pinned(ctx, name) {
		return nil, contracts.ErrQueueNotFound
	}
	s.sent = append(s.sent, msgs...)
	return []contracts.SendResult{{MessageID: "m-1"}}, nil
}

// s3Policy allows s3.amazonaws.com sqs:SendMessage on orders.
const s3Policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
	"Principal":{"Service":"s3.amazonaws.com"},"Action":"sqs:SendMessage",
	"Resource":"` + ordersARN + `"}]}`

// serveSend runs the endpoint's handler on store with body, and returns the
// status and the body answered.
func serveSend(t *testing.T, store *sendStore, body string) (int, string) {
	t.Helper()
	settings := &contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"}
	az := authz.New(settings.AccountID, settings.Region, time.Now)
	m := NewModule(settings, az, nil)(nil, store)
	r := httptest.NewRequest(http.MethodPost, "http://queues.local"+servicesend.Path,
		strings.NewReader(body))
	w := httptest.NewRecorder()
	if err := m.serviceSend(gas.NewContext(r.Context(), w, r)); err != nil {
		t.Fatalf("serve: %v", err)
	}
	return w.Code, w.Body.String()
}

func sendBody(body string) string {
	return `{"QueueArn":"` + ordersARN + `","ServicePrincipal":"s3.amazonaws.com",` +
		`"MessageBody":` + body + `}`
}

func TestTheServiceSendSendsAsTheService(t *testing.T) {
	store := &sendStore{policy: s3Policy, evaluated: 7, current: 7}
	status, got := serveSend(t, store, sendBody("\"<b>&\\u2028</b>\""))
	if status != http.StatusOK || len(store.sent) != 1 {
		t.Fatalf("the evaluated queue: %d %s, sent %d", status, got, len(store.sent))
	}
	in := store.sent[0]
	if in.Body != "<b>&\u2028</b>" || in.SenderID != s3Service ||
		in.MD5OfBody != sqsapi.MD5OfBody(in.Body) {
		t.Fatalf("sent %+v", in)
	}
	var res servicesend.Response
	if err := json.Unmarshal([]byte(got), &res); err != nil || res.MessageID != "m-1" ||
		res.MD5OfMessageBody != in.MD5OfBody {
		t.Fatalf("answer %s, %v", got, err)
	}
}

func TestTheServiceSendIsPinnedToTheEvaluatedQueue(t *testing.T) {
	// Deleted and created again after the evaluation: not sent to.
	store := &sendStore{policy: s3Policy, evaluated: 7, current: 8}
	status, got := serveSend(t, store, sendBody(`"x"`))
	if status != http.StatusNotFound || !strings.Contains(got, servicesend.CodeResourceNotFound) ||
		len(store.sent) != 0 {
		t.Fatalf("a recreated queue: %d %s, sent %d", status, got, len(store.sent))
	}
}

func TestTheMessageIsCheckedOnlyOnceTheServiceIsAllowed(t *testing.T) {
	for what, c := range map[string]struct {
		policy string
		body   string
		code   string
		status int
	}{
		"a lone surrogate, allowed":   {s3Policy, `"\ud800"`, servicesend.CodeInvalidMessage, 400},
		"a lone surrogate, denied":    {"", `"\ud800"`, servicesend.CodeAccessDenied, 403},
		"an invalid character":        {s3Policy, `"\u0001"`, servicesend.CodeInvalidMessage, 400},
		"no body, allowed":            {s3Policy, `null`, servicesend.CodeInvalidMessage, 400},
		"no body, denied":             {"", `""`, servicesend.CodeAccessDenied, 403},
		"a body that is not a string": {s3Policy, `1`, servicesend.CodeInvalidRequest, 400},
	} {
		store := &sendStore{policy: c.policy, evaluated: 1, current: 1}
		status, got := serveSend(t, store, sendBody(c.body))
		var e servicesend.ErrorResponse
		if err := json.Unmarshal([]byte(got), &e); err != nil || status != c.status ||
			e.Code != c.code || len(store.sent) != 0 {
			t.Errorf("%s: %d %s, sent %d; want %d %s", what, status, got, len(store.sent),
				c.status, c.code)
		}
	}
	// A body that is not UTF-8 is refused before it is decoded.
	store := &sendStore{policy: s3Policy, evaluated: 1, current: 1}
	if status, got := serveSend(t, store, sendBody("\"\xff\"")); status != http.StatusBadRequest ||
		!strings.Contains(got, servicesend.CodeInvalidRequest) {
		t.Errorf("a body that is not UTF-8: %d %s", status, got)
	}
}
