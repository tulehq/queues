package sqlimpl

import (
	"errors"
	"testing"
	"time"

	"tule.dev/queues/internal/contracts"
)

func TestAPinnedQueueIsTheEvaluatedOne(t *testing.T) {
	s, clk := newStore(t)
	createQueue(t, s, "orders", false, nil)
	if err := s.TagQueue(t.Context(), "orders", map[string]string{"team": "blue"}); err != nil {
		t.Fatalf("tag: %v", err)
	}
	id, tags, err := s.QueueTags(t.Context(), "orders")
	if err != nil || tags["team"] != "blue" || id == 0 {
		t.Fatalf("QueueTags = %d, %v, %v", id, tags, err)
	}
	pinned := contracts.WithQueuePin(t.Context(), "orders", id)
	if _, err = s.QueueInfo(pinned, "orders"); err != nil {
		t.Fatalf("the pinned queue itself: %v", err)
	}

	// Deleted and created again under its name between the evaluation and
	// the operation, it is another queue, and the pinned operation does not
	// act on it.
	recreate(t, s, clk, "orders")
	for name, call := range map[string]func() error{
		"QueueInfo": func() error { _, e := s.QueueInfo(pinned, "orders"); return e },
		"SendMessages": func() error {
			_, e := s.SendMessages(pinned, "orders", []contracts.SendInput{{Body: "x"}})
			return e
		},
		"TagQueue":    func() error { return s.TagQueue(pinned, "orders", map[string]string{"a": "b"}) },
		"PurgeQueue":  func() error { return s.PurgeQueue(pinned, "orders") },
		"DeleteQueue": func() error { return s.DeleteQueue(pinned, "orders") },
	} {
		if err = call(); !errors.Is(err, contracts.ErrQueueNotFound) {
			t.Errorf("%s on the recreated queue: %v, want ErrQueueNotFound", name, err)
		}
	}
	if _, err = s.QueueInfo(t.Context(), "orders"); err != nil {
		t.Fatalf("the recreated queue, unpinned: %v", err)
	}
}

// recreate deletes the queue name and creates it again, tagged, once the
// 60 seconds a deleted name is held have passed.
func recreate(t *testing.T, s *Service, clk *clock, name string) {
	t.Helper()
	if err := s.DeleteQueue(t.Context(), name); err != nil {
		t.Fatalf("delete: %v", err)
	}
	clk.advance(61 * time.Second)
	createQueue(t, s, name, false, nil)
	if err := s.TagQueue(t.Context(), name, map[string]string{"team": "red"}); err != nil {
		t.Fatalf("tag the new queue: %v", err)
	}
}

func TestAPinMatchesOnlyItsQueueName(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	id, _, err := s.QueueTags(t.Context(), "orders")
	if err != nil {
		t.Fatalf("QueueTags: %v", err)
	}
	pinned := contracts.WithQueuePin(t.Context(), "orders", id)

	// A pin of a queue that did not exist keeps it not found.
	absent := contracts.WithQueuePin(t.Context(), "orders", 0)
	if _, err = s.QueueInfo(absent, "orders"); !errors.Is(err, contracts.ErrQueueNotFound) {
		t.Fatalf("a queue created after an evaluation found none: %v", err)
	}
	// A pin names one queue; others resolve as usual.
	createQueue(t, s, "other", false, nil)
	if _, err = s.QueueInfo(pinned, "other"); err != nil {
		t.Fatalf("another queue under the pin: %v", err)
	}
}

func TestAPinnedTagQueueRefusesKeysDifferingOnlyByCase(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "orders", false, nil)
	id, _, err := s.QueueTags(t.Context(), "orders")
	if err != nil {
		t.Fatalf("QueueTags: %v", err)
	}
	pinned := contracts.WithQueuePin(t.Context(), "orders", id)

	// Two evaluated requests that each passed authorization's check, with
	// the queue untagged: the second is refused in its own transaction.
	if err = s.TagQueue(pinned, "orders", map[string]string{"Env": "a"}); err != nil {
		t.Fatalf("the first: %v", err)
	}
	requireCaseError(t, s.TagQueue(pinned, "orders", map[string]string{"env": "b"}), "the second")
	requireCaseError(t, s.TagQueue(pinned, "orders", map[string]string{"x": "1", "X": "2"}),
		"two keys of one request")
	// The same key overwrites.
	if err = s.TagQueue(pinned, "orders", map[string]string{"Env": "c"}); err != nil {
		t.Fatalf("overwrite Env: %v", err)
	}
	tags, err := s.ListQueueTags(t.Context(), "orders")
	if err != nil || len(tags) != 1 || tags["Env"] != "c" {
		t.Fatalf("tags = %v, %v", tags, err)
	}

	// A root request carries no pin and is unchanged.
	if err = s.TagQueue(t.Context(), "orders", map[string]string{"env": "b"}); err != nil {
		t.Fatalf("an unpinned TagQueue: %v", err)
	}
}

func requireCaseError(t *testing.T, err error, what string) {
	t.Helper()
	var caseErr *contracts.TagKeyCaseError
	if !errors.As(err, &caseErr) {
		t.Fatalf("%s: %v, want a TagKeyCaseError", what, err)
	}
}
