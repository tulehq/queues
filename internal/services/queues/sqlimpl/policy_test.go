package sqlimpl

import (
	"context"
	"errors"
	"strings"
	"testing"

	"tule.dev/queues/internal/contracts"
)

// ordersPolicy returns the orders queue's Policy.
func ordersPolicy(t *testing.T, s *Service) string {
	t.Helper()
	q, err := s.QueueInfo(t.Context(), "orders")
	if err != nil {
		t.Fatalf("queue orders: %v", err)
	}
	return q.Attributes[contracts.AttrPolicy]
}

func permission(label string) contracts.Permission {
	return contracts.Permission{
		Label: label, AccountIDs: []string{"111122223333"}, Actions: []string{"SendMessage"},
	}
}

func TestAPermissionChangeStoresWhatBindReturns(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	var asked []string
	bind := func(_ context.Context, doc string) (string, error) {
		asked = append(asked, doc)
		// The account's root ARN becomes the account ID, as a stand-in for
		// an ARN becoming a unique ID.
		return strings.ReplaceAll(doc, `"arn:aws:iam::111122223333:root"`, `"111122223333"`), nil
	}
	if err := s.AddPermission(t.Context(), "orders", permission("a"), bind); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	if got := ordersPolicy(t, s); strings.Contains(got, ":root") ||
		len(asked) != 1 || !strings.Contains(asked[0], ":root") {
		t.Fatalf("stored %s after binding %q", got, asked)
	}
	if err := s.AddPermission(t.Context(), "orders", permission("b"), bind); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	if err := s.RemovePermission(t.Context(), "orders", "a", bind); err != nil {
		t.Fatalf("RemovePermission: %v", err)
	}
	if len(asked) != 3 {
		t.Fatalf("bind asked %d times, want 3", len(asked))
	}
}

func TestRemovingTheLastStatementBindsNothing(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	if err := s.AddPermission(t.Context(), "orders", permission("a"), nil); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	err := s.RemovePermission(t.Context(), "orders", "a",
		func(context.Context, string) (string, error) {
			t.Error("bind asked to bind no policy")
			return "", nil
		})
	if err != nil {
		t.Fatalf("RemovePermission: %v", err)
	}
	if got := ordersPolicy(t, s); got != "" {
		t.Fatalf("Policy %q after the last removal", got)
	}
}

func TestARefusedBindStoresNothing(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	refused := &contracts.PolicyError{Reason: "Invalid principal in policy"}
	err := s.AddPermission(t.Context(), "orders", permission("a"),
		func(context.Context, string) (string, error) { return "", refused })
	if !errors.Is(err, refused) {
		t.Fatalf("error = %v, want the bind's", err)
	}
	if got := ordersPolicy(t, s); got != "" {
		t.Fatalf("Policy %q after a refused bind", got)
	}
}

func TestAPolicyWrittenDuringABindIsNotLost(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	// The first bind lets another AddPermission in before the edit is
	// written: the edit starts again on the policy holding it.
	calls := 0
	bind := func(ctx context.Context, doc string) (string, error) {
		calls++
		if calls == 1 {
			if err := s.AddPermission(ctx, "orders", permission("other"), nil); err != nil {
				t.Errorf("concurrent AddPermission: %v", err)
			}
		}
		return doc, nil
	}
	if err := s.AddPermission(t.Context(), "orders", permission("mine"), bind); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	got := ordersPolicy(t, s)
	if calls != 2 || !strings.Contains(got, `"Sid":"other"`) ||
		!strings.Contains(got, `"Sid":"mine"`) {
		t.Fatalf("after %d binds the Policy is %s", calls, got)
	}
}

func TestAnEditOvertakenEveryTimeGivesUp(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	n := 0
	err := s.AddPermission(t.Context(), "orders", permission("never"),
		func(ctx context.Context, doc string) (string, error) {
			n++
			if aErr := s.AddPermission(ctx, "orders", permission("o"+strings.Repeat("x", n)),
				nil); aErr != nil {
				t.Errorf("concurrent AddPermission: %v", aErr)
			}
			return doc, nil
		})
	if !errors.Is(err, errPolicyContended) || n != maxPolicyEdits ||
		strings.Contains(ordersPolicy(t, s), `"Sid":"never"`) {
		t.Fatalf("error = %v after %d attempts", err, n)
	}
}
