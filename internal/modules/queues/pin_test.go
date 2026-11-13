package queues

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gasmod/gas"
	"tule.dev/sigv4"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

// pinStore is a store that records the pin each queue lookup was made
// under. Only the methods the test calls are implemented.
type pinStore struct {
	contracts.QueueProvider
	pins []contracts.QueueID
}

const evaluatedID contracts.QueueID = 7

func (s *pinStore) QueueAccess(context.Context, string) (*contracts.QueueAccess, error) {
	return &contracts.QueueAccess{ID: evaluatedID, Tags: map[string]string{}}, nil
}

func (s *pinStore) QueueInfo(ctx context.Context, name string) (*contracts.Queue, error) {
	id, ok := contracts.PinnedQueue(ctx, name)
	if !ok {
		id = -1
	}
	s.pins = append(s.pins, id)
	return nil, contracts.ErrQueueNotFound
}

func TestTheOperationRunsPinnedToTheEvaluatedQueue(t *testing.T) {
	settings := &contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"}
	store := &pinStore{}
	m := NewModule(settings, authz.New(settings.AccountID, settings.Region, time.Now))(nil, store)

	body := `{"QueueUrl":"http://queues.local/000000000000/orders","MessageBody":"x"}`
	r := httptest.NewRequest(http.MethodPost, "http://queues.local/", strings.NewReader(body))
	r = r.WithContext(sigv4.NewContext(r.Context(), sigv4.Principal{
		AccountID: "000000000000", ARN: "arn:aws:iam::000000000000:user/dev",
		PrincipalARN: "arn:aws:iam::000000000000:user/dev", UserID: "AIDAEXAMPLEUSERID01",
		Type: sigv4.PrincipalTypeUser,
		Policies: []json.RawMessage{
			json.RawMessage(
				`{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"sqs:*","Resource":"*"}}`,
			),
		},
	}))
	c := &call{
		Context: gas.NewContext(r.Context(), httptest.NewRecorder(), r),
		op:      "SendMessage", body: []byte(body), proto: sqsapi.JSON,
	}

	if err := m.authorize(c); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if _, err := m.handlers[c.op](c); !errors.Is(err, contracts.ErrQueueNotFound) {
		t.Fatalf("send: %v", err)
	}
	if len(store.pins) != 1 || store.pins[0] != evaluatedID {
		t.Fatalf(
			"the operation resolved its queue under pins %v, want [%d]",
			store.pins,
			evaluatedID,
		)
	}
}

// runHandler runs h, reporting the error it returns before it reaches the
// store; a call into the nil store panics, which means the handler got past
// its decoding.
func runHandler(h handler, c *call) (err error) {
	defer func() {
		if recover() != nil {
			err = nil
		}
	}()
	_, err = h(c)
	return err
}

func TestAuthorizationAndTheOperationShareOneDecoding(t *testing.T) {
	settings := &contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"}
	m := NewModule(settings, authz.New(settings.AccountID, settings.Region, time.Now))(nil, nil)
	body := `{"QueueUrl":"http://queues.local/000000000000/orders","QueueName":"orders"}`

	for op, a := range operationAccess {
		r := httptest.NewRequest(http.MethodPost, "http://queues.local/", strings.NewReader(body))
		c := &call{
			Context: gas.NewContext(r.Context(), httptest.NewRecorder(), r),
			op:      op, body: []byte(body), proto: sqsapi.JSON,
		}
		if _, err := a.target(m, c); err != nil {
			t.Fatalf("%s: target: %v", op, err)
		}
		evaluated := c.req
		if err := runHandler(m.handlers[op], c); errors.Is(err, errDecodedAs) {
			t.Errorf("%s: the handler decodes another type than its target: %v", op, err)
		}
		// ListQueues' authorization reads nothing of the request.
		if evaluated != nil && c.req != evaluated {
			t.Errorf("%s: the handler decoded again", op)
		}
	}
}
