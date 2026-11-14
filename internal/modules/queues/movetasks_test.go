package queues

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gasmod/gas"
	"tule.dev/sigv4"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

// moveStore is a store for authorizing the move task operations: every
// queue exists with no tags and no policy, and a dead-letter queue's
// messages came from origins.
type moveStore struct {
	contracts.QueueProvider
	origins []string
}

func (s *moveStore) QueueAccess(context.Context, string) (*contracts.QueueAccess, error) {
	return &contracts.QueueAccess{ID: evaluatedID, Tags: map[string]string{}}, nil
}

func (s *moveStore) MoveScope(context.Context, string) (*contracts.MoveScope, error) {
	return &contracts.MoveScope{Origins: s.origins, UpTo: 1}, nil
}

var moveSettings = &contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"}

// grants is the identity policy allowing each queue's actions, by name.
type grants map[string][]string

func (g grants) policy(t *testing.T) json.RawMessage {
	t.Helper()
	stmts := make([]map[string]any, 0, len(g))
	for name, actions := range g {
		stmts = append(stmts, map[string]any{
			"Effect": "Allow", "Action": actions, "Resource": moveSettings.ARN(name),
		})
	}
	raw, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": stmts})
	if err != nil {
		t.Fatalf("encode the policy: %v", err)
	}
	return raw
}

// authorizeMove authorizes the JSON request body to op for a user of the
// account whose identity policy is g's.
func authorizeMove(t *testing.T, store *moveStore, op, body string, g grants) error {
	t.Helper()
	az := authz.New(moveSettings.AccountID, moveSettings.Region, time.Now)
	m := NewModule(moveSettings, az, nil)(nil, store)
	r := httptest.NewRequest(http.MethodPost, "http://queues.local/", strings.NewReader(body))
	r = r.WithContext(sigv4.NewContext(r.Context(), sigv4.Principal{
		AccountID: "000000000000", ARN: "arn:aws:iam::000000000000:user/dev",
		PrincipalARN: "arn:aws:iam::000000000000:user/dev", UserID: "AIDAEXAMPLEUSERID01",
		Type: sigv4.PrincipalTypeUser, Policies: []json.RawMessage{g.policy(t)},
	}))
	c := &call{
		Context: gas.NewContext(r.Context(), httptest.NewRecorder(), r),
		op:      op, body: []byte(body), proto: sqsapi.JSON,
	}
	return m.authorize(c)
}

// requireDenied fails unless err denies action on the queue name.
func requireDenied(t *testing.T, err error, action, name string) {
	t.Helper()
	var d *authz.Denied
	if !errors.As(err, &d) || d.Action != action || d.Resource != moveSettings.ARN(name) {
		t.Fatalf("authorize = %v, want %s denied on %s", err, action, name)
	}
}

func without(actions []string, drop string) []string {
	return slices.DeleteFunc(slices.Clone(actions), func(a string) bool { return a == drop })
}

func TestTheMoveTaskOperationsNeedEveryActionOnTheSource(t *testing.T) {
	dlq := moveSettings.ARN("dlq")
	for _, tt := range []struct {
		others   grants
		op, body string
		actions  []string
	}{
		{
			op:      "StartMessageMoveTask",
			body:    `{"SourceArn":"` + dlq + `","DestinationArn":"` + moveSettings.ARN("dest") + `"}`,
			actions: append([]string{"sqs:StartMessageMoveTask"}, moveSourceActions...),
			others:  grants{"dest": {"sqs:SendMessage"}},
		},
		{
			op:      "CancelMessageMoveTask",
			body:    `{"TaskHandle":"` + contracts.TaskHandle("t", dlq) + `"}`,
			actions: append([]string{"sqs:CancelMessageMoveTask"}, moveSourceActions...),
		},
		{
			op:      "ListMessageMoveTasks",
			body:    `{"SourceArn":"` + dlq + `"}`,
			actions: []string{"sqs:ListMessageMoveTasks", "sqs:GetQueueAttributes"},
		},
	} {
		all := grants{"dlq": tt.actions}
		for name, actions := range tt.others {
			all[name] = actions
		}
		if err := authorizeMove(t, &moveStore{}, tt.op, tt.body, all); err != nil {
			t.Fatalf("%s with every action: %v", tt.op, err)
		}
		for _, drop := range tt.actions {
			g := grants{"dlq": without(tt.actions, drop)}
			for name, actions := range tt.others {
				g[name] = actions
			}
			requireDenied(t, authorizeMove(t, &moveStore{}, tt.op, tt.body, g), drop, "dlq")
		}
	}
}

func TestAStartNeedsSendMessageOnItsDestination(t *testing.T) {
	body := `{"SourceArn":"` + moveSettings.ARN("dlq") + `","DestinationArn":"` +
		moveSettings.ARN("dest") + `"}`
	source := append([]string{"sqs:StartMessageMoveTask"}, moveSourceActions...)
	err := authorizeMove(t, &moveStore{origins: []string{"origin"}}, "StartMessageMoveTask",
		body, grants{"dlq": source, "origin": {"sqs:SendMessage"}})
	requireDenied(t, err, "sqs:SendMessage", "dest")
}

func TestAStartWithoutADestinationNeedsSendMessageOnEveryOrigin(t *testing.T) {
	body := `{"SourceArn":"` + moveSettings.ARN("dlq") + `"}`
	source := append([]string{"sqs:StartMessageMoveTask"}, moveSourceActions...)
	store := &moveStore{origins: []string{"a", "b"}}
	send := []string{"sqs:SendMessage"}

	if err := authorizeMove(t, store, "StartMessageMoveTask", body,
		grants{"dlq": source, "a": send, "b": send}); err != nil {
		t.Fatalf("start with SendMessage on every origin: %v", err)
	}
	requireDenied(t, authorizeMove(t, store, "StartMessageMoveTask", body,
		grants{"dlq": source, "a": send}), "sqs:SendMessage", "b")
}
