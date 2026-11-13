// Package contracts defines the queue store interface the HTTP modules depend
// on, with its input and output types and sentinel errors.
package contracts

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrQueueNotFound is returned when no queue has the given name.
	ErrQueueNotFound = errors.New("queues: queue not found")

	// ErrQueueExists is returned by CreateQueue when a queue of the name
	// exists with an attribute of another value than the request gives.
	ErrQueueExists = errors.New("queues: queue exists with other attributes")

	// ErrQueueDeletedRecently is returned by CreateQueue for a name whose
	// queue DeleteQueue deleted within the last QueueDeleteWindow.
	ErrQueueDeletedRecently = errors.New("queues: queue of the name deleted recently")

	// ErrPurgeInProgress is returned by PurgeQueue within PurgeWindow of the
	// queue's last purge.
	ErrPurgeInProgress = errors.New("queues: queue purged recently")

	// ErrMessageNotInflight is returned by ChangeMessageVisibility for a
	// receipt handle of a receive whose message is no longer in flight under
	// it: its visibility timeout ended, or the message was received again.
	ErrMessageNotInflight = errors.New("queues: message not in flight")

	// ErrInvalidToken is returned by the listings for a NextToken they did
	// not issue.
	ErrInvalidToken = errors.New("queues: invalid page token")
)

// ReceiptError is returned for a receipt handle that names no receive of a
// message of the queue: malformed, issued for another queue, or of a message
// that is gone.
type ReceiptError struct {
	Handle string
}

func (e *ReceiptError) Error() string {
	return "queues: receipt handle names no message of the queue"
}

// InvalidError is returned for a parameter value the store refuses, such as
// a RedrivePolicy whose dead-letter queue does not exist.
type InvalidError struct {
	Field  string
	Value  string
	Reason string
}

func (e *InvalidError) Error() string {
	return "queues: invalid " + e.Field + ": " + e.Reason
}

// The windows SQS documents for its operations.
const (
	// QueueDeleteWindow is how long a deleted queue's name stays taken.
	QueueDeleteWindow = 60 * time.Second
	// PurgeWindow is how long after a purge another is refused.
	PurgeWindow = 60 * time.Second
	// DeduplicationInterval is how long a FIFO queue remembers a
	// deduplication ID.
	DeduplicationInterval = 5 * time.Minute
	// ReceiveAttemptWindow is how long a FIFO queue remembers a
	// ReceiveRequestAttemptId.
	ReceiveAttemptWindow = 5 * time.Minute
	// MaxVisibility is the longest a message may stay in flight under one
	// receive, from when it was received.
	MaxVisibility = 12 * time.Hour
)

// The states a message is in.
const (
	StateDelayed  = "delayed"
	StateVisible  = "visible"
	StateInFlight = "inFlight"
)

// Queue is a queue with its attributes and message counts.
type Queue struct {
	CreatedAt  time.Time
	ModifiedAt time.Time
	// Attributes are the queue's settable attributes, every one with a
	// default included, as GetQueueAttributes reports them. An optional
	// attribute that is not set, such as RedrivePolicy, is absent.
	Attributes map[string]string
	Name       string
	Counts     Counts
	FIFO       bool
}

// Counts are the numbers of a queue's messages in each state.
type Counts struct {
	Visible  int
	InFlight int
	Delayed  int
}

// CreateQueueInput is a queue to create.
type CreateQueueInput struct {
	// Attributes are the attributes the request sets, already checked and
	// normalized; the store fills in the defaults of the rest.
	Attributes map[string]string
	Tags       map[string]string
	Name       string
	FIFO       bool
}

// MessageAttribute is a message attribute or a message system attribute.
type MessageAttribute struct {
	StringValue *string
	DataType    string
	BinaryValue []byte
}

// SendInput is one message to send, already checked against the queue.
type SendInput struct {
	Attributes       map[string]MessageAttribute
	SystemAttributes map[string]MessageAttribute
	// DelaySeconds is nil for the queue's delay.
	DelaySeconds *int
	Body         string
	MD5OfBody    string
	// MD5OfAttributes and MD5OfSystemAttributes are "" when there are no
	// attributes of the kind.
	MD5OfAttributes       string
	MD5OfSystemAttributes string
	SenderID              string
	GroupID               string
	// DeduplicationID is the FIFO deduplication ID, the request's or the
	// content-based one; "" on a standard queue.
	DeduplicationID string
}

// SendResult is what sending one message produced. A FIFO message the queue
// deduplicated is reported with the ID and sequence number of the message it
// duplicates.
type SendResult struct {
	MessageID      string
	SequenceNumber string
}

// ReceiveInput is a ReceiveMessage request.
type ReceiveInput struct {
	// VisibilityTimeout and WaitSeconds are nil for the queue's own.
	VisibilityTimeout *int
	WaitSeconds       *int
	AttemptID         string
	Max               int
}

// Message is a stored message. ReceiptHandle is set on a message a receive
// returned.
type Message struct {
	SentAt           time.Time
	VisibleAt        time.Time
	FirstReceivedAt  time.Time
	Attributes       map[string]MessageAttribute
	SystemAttributes map[string]MessageAttribute
	ID               string
	Body             string
	MD5OfBody        string
	MD5OfAttributes  string
	MD5OfSystem      string
	SenderID         string
	GroupID          string
	DeduplicationID  string
	SequenceNumber   string
	// DeadLetterSourceARN is the ARN of the queue a message moved to its
	// dead-letter queue from.
	DeadLetterSourceARN string
	ReceiptHandle       string
	State               string
	ReceiveCount        int
}

// VisibilityChange is one ChangeMessageVisibility.
type VisibilityChange struct {
	ReceiptHandle string
	Timeout       int
}

// Permission is one AddPermission statement.
type Permission struct {
	Label      string
	AccountIDs []string
	Actions    []string
}

// MessageFilter narrows an inspection listing.
type MessageFilter struct {
	State string
	Group string
}

// PageInput asks for a page of a listing. MaxResults 0 asks for the
// listing's default.
type PageInput struct {
	NextToken  string
	MaxResults int
}

// Page is one page of a listing, with the token of the next or "" on the
// last.
type Page[T any] struct {
	NextToken string
	Items     []T
}

// QueueProvider stores the queues and their messages.
type QueueProvider interface {
	CreateQueue(ctx context.Context, in CreateQueueInput) (*Queue, error)
	// GetQueue returns the queue with its counts.
	GetQueue(ctx context.Context, name string) (*Queue, error)
	// QueueInfo returns the queue without its counts, which cost a read of
	// its messages: what a send or an attribute change needs.
	QueueInfo(ctx context.Context, name string) (*Queue, error)
	DeleteQueue(ctx context.Context, name string) error
	// ListQueues lists queue names in order, those starting with prefix.
	ListQueues(ctx context.Context, prefix string, page PageInput) (*Page[string], error)
	SetQueueAttributes(ctx context.Context, name string, attrs map[string]string) error
	PurgeQueue(ctx context.Context, name string) error
	TagQueue(ctx context.Context, name string, tags map[string]string) error
	UntagQueue(ctx context.Context, name string, keys []string) error
	ListQueueTags(ctx context.Context, name string) (map[string]string, error)
	// ListDeadLetterSourceQueues lists the names of the queues whose
	// RedrivePolicy targets the queue.
	ListDeadLetterSourceQueues(ctx context.Context, name string,
		page PageInput) (*Page[string], error)
	AddPermission(ctx context.Context, name string, p Permission) error
	RemovePermission(ctx context.Context, name, label string) error

	// SendMessages sends messages in order, all or none.
	SendMessages(ctx context.Context, name string, msgs []SendInput) ([]SendResult, error)
	// ReceiveMessages receives up to in.Max messages, waiting up to the
	// wait time for one to arrive when none is available.
	ReceiveMessages(ctx context.Context, name string, in ReceiveInput) ([]*Message, error)
	// DeleteMessages deletes the messages of receipt handles, returning one
	// error, or nil, per handle.
	DeleteMessages(ctx context.Context, name string, handles []string) ([]error, error)
	// ChangeVisibility changes the visibility timeouts of received
	// messages, returning one error, or nil, per change.
	ChangeVisibility(ctx context.Context, name string, changes []VisibilityChange) ([]error, error)

	// ListMessages lists a queue's messages in queue order without
	// receiving them.
	ListMessages(ctx context.Context, name string, f MessageFilter,
		page PageInput) (*Page[*Message], error)
	// ListAllQueues returns every queue, in name order.
	ListAllQueues(ctx context.Context) ([]*Queue, error)
	// PurgeAll deletes every message of every queue.
	PurgeAll(ctx context.Context) error
	// DeleteAll deletes every queue.
	DeleteAll(ctx context.Context) error
}
