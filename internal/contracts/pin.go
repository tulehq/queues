package contracts

import "context"

// QueueID is a queue's identity in the store. IDs are never reused, so a
// queue deleted and created again under its name has a new one.
type QueueID int64

type pinKey struct{}

type queuePin struct {
	name string
	id   QueueID
}

// WithQueuePin returns ctx pinning the queue name to id: the queue a
// request was evaluated on. A store resolving name under ctx answers
// ErrQueueNotFound for a queue of that name with another ID, so an
// operation never acts on a queue created after the evaluation in place of
// the evaluated one. id 0 pins the name to no queue, for a request
// evaluated when the queue did not exist.
func WithQueuePin(ctx context.Context, name string, id QueueID) context.Context {
	return context.WithValue(ctx, pinKey{}, queuePin{name: name, id: id})
}

// PinnedQueue returns the ID ctx pins name to, and whether it pins it.
func PinnedQueue(ctx context.Context, name string) (QueueID, bool) {
	p, ok := ctx.Value(pinKey{}).(queuePin)
	if !ok || p.name != name {
		return 0, false
	}
	return p.id, true
}
