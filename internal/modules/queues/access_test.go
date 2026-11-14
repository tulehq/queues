package queues

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

// serviceReference is the part of the SQS Service Authorization Reference
// the mapping is checked against: testdata/sqs-service-reference.json, as
// https://servicereference.us-east-1.amazonaws.com/v1/sqs/sqs.json served
// it on 2026-11-13.
type serviceReference struct {
	Actions []struct {
		Name      string
		Resources []struct{ Name string }
	}
	Operations []struct {
		Name              string
		AuthorizedActions []struct{ Name, Service string }
	}
}

func loadReference(t *testing.T) serviceReference {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "sqs-service-reference.json"))
	if err != nil {
		t.Fatalf("read the reference: %v", err)
	}
	var ref serviceReference
	if err = json.Unmarshal(raw, &ref); err != nil {
		t.Fatalf("decode the reference: %v", err)
	}
	return ref
}

func routedOperations() []string {
	m := NewModule(nil, nil, nil)(nil, nil)
	return slices.Sorted(maps.Keys(m.handlers))
}

func TestEveryRoutedOperationHasAnAccessEntry(t *testing.T) {
	ops := routedOperations()
	for _, op := range ops {
		if _, ok := operationAccess[op]; !ok {
			t.Errorf("%s is routed but has no access entry", op)
		}
	}
	for op := range operationAccess {
		if !slices.Contains(ops, op) {
			t.Errorf("%s has an access entry but is not routed", op)
		}
	}
}

// referenceIndex returns each action's resource types and each
// operation's authorized actions.
func referenceIndex(ref serviceReference) (resources, authorized map[string][]string) {
	resources, authorized = map[string][]string{}, map[string][]string{}
	for _, a := range ref.Actions {
		names := make([]string, 0, len(a.Resources))
		for _, r := range a.Resources {
			names = append(names, r.Name)
		}
		resources["sqs:"+a.Name] = names
	}
	for _, o := range ref.Operations {
		for _, a := range o.AuthorizedActions {
			authorized[o.Name] = append(authorized[o.Name], a.Service+":"+a.Name)
		}
	}
	return resources, authorized
}

func TestEveryEntryIsTheReferencesAction(t *testing.T) {
	resources, authorized := referenceIndex(loadReference(t))
	for op, a := range operationAccess {
		res, ok := resources[a.action]
		if !ok {
			t.Errorf("%s: %s is not an action of the reference", op, a.action)
			continue
		}
		if !slices.Contains(authorized[op], a.action) {
			t.Errorf("%s: the reference authorizes it as %v, not %s", op, authorized[op], a.action)
		}
		// The reference's only resource type is the queue; an action with
		// none is evaluated on "*".
		wantAny := len(res) == 0
		gotAny := reflect.ValueOf(a.target).Pointer() == reflect.ValueOf(anyResource).Pointer()
		if wantAny != gotAny {
			t.Errorf("%s: resources %v, but the entry evaluates on \"*\": %v", op, res, gotAny)
		}
		// Any further action the reference lists must be one the entry
		// checks too: CreateQueue's sqs:TagQueue, when it tags, the
		// actions an entry also checks on its queue, and
		// StartMessageMoveTask's sqs:SendMessage on the queues it sends to.
		for _, extra := range authorized[op] {
			if extra != a.action && !alsoChecked(op, a, extra) {
				t.Errorf("%s: the reference also authorizes it as %s, which is not checked",
					op, extra)
			}
		}
	}
}

// alsoChecked reports whether the entry a of op checks extra beside its
// action.
func alsoChecked(op string, a access, extra string) bool {
	return op == "CreateQueue" && extra == tagQueueAction ||
		slices.Contains(a.also, extra) ||
		op == "StartMessageMoveTask" && extra == sendMessageAction
}

func TestTheOwnerOnlyOperationsAreRouted(t *testing.T) {
	// SQS Developer Guide, "Basic examples of Amazon SQS policies",
	// Example 4: the routed operations cross-account permissions don't
	// apply to.
	want := []string{
		"AddPermission", "CancelMessageMoveTask", "CreateQueue", "DeleteQueue",
		"ListMessageMoveTasks", "ListQueues", "ListQueueTags", "RemovePermission",
		"SetQueueAttributes", "StartMessageMoveTask", "TagQueue", "UntagQueue",
	}
	if len(ownerOnly) != len(want) {
		t.Fatalf("ownerOnly has %d operations, want %d", len(ownerOnly), len(want))
	}
	for _, op := range want {
		if _, routed := operationAccess[op]; !ownerOnly[op] || !routed {
			t.Errorf("%s: owner only %t, routed %t", op, ownerOnly[op], routed)
		}
	}
}
