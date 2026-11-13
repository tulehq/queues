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

func TestAddPermissionBindsOnlyItsStatement(t *testing.T) {
	s, _ := newStore(t)
	stored := `{"Version":"2012-10-17","Statement":[{"Sid":"Dev","Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::000000000000:user/dev"},` +
		`"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:us-east-1:000000000000:orders"}]}`
	createQueue(t, s, "orders", false, map[string]string{contracts.AttrPolicy: stored})
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
	if len(asked) != 1 || strings.Contains(asked[0], "user/dev") ||
		!strings.Contains(asked[0], `"Sid":"a"`) {
		t.Fatalf("bind asked %q, want the generated statement alone", asked)
	}
	got := ordersPolicy(t, s)
	if strings.Contains(got, ":root") || !strings.Contains(got, `"111122223333"`) ||
		!strings.Contains(got, `"arn:aws:iam::000000000000:user/dev"`) {
		t.Fatalf("stored %s", got)
	}
}

func TestRemovePermissionStoresTheRestAsItWas(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	for _, label := range []string{"a", "b"} {
		if err := s.AddPermission(t.Context(), "orders", permission(label), nil); err != nil {
			t.Fatalf("AddPermission %s: %v", label, err)
		}
	}
	if err := s.RemovePermission(t.Context(), "orders", "a"); err != nil {
		t.Fatalf("RemovePermission: %v", err)
	}
	got := ordersPolicy(t, s)
	if strings.Contains(got, `"Sid":"a"`) || !strings.Contains(got, `"Sid":"b"`) {
		t.Fatalf("Policy after removing a: %s", got)
	}
	// Removing the last statement removes the Policy.
	if err := s.RemovePermission(t.Context(), "orders", "b"); err != nil {
		t.Fatalf("RemovePermission: %v", err)
	}
	if got = ordersPolicy(t, s); got != "" {
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
