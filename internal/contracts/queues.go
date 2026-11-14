// Package contracts defines the queue store interface the HTTP modules depend
// on, with its input and output types and sentinel errors.
package contracts

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

	// ErrMoveTaskNotFound is returned by CancelMoveTask for a task handle
	// that names no task of the queue.
	ErrMoveTaskNotFound = errors.New("queues: message move task not found")

	// ErrNotDeadLetterQueue is returned by StartMoveTask for a source queue
	// that no queue's RedrivePolicy names as its dead-letter queue.
	ErrNotDeadLetterQueue = errors.New("queues: queue is not a dead-letter queue")

	// ErrMoveTaskActive is returned by StartMoveTask for a source queue that
	// has a task running or being canceled.
	ErrMoveTaskActive = errors.New("queues: queue has an active message move task")

	// ErrMoveTaskNotRunning is returned by CancelMoveTask for a task that is
	// not running.
	ErrMoveTaskNotRunning = errors.New("queues: message move task not running")
)

// TagKeyCaseError is a TagQueue under a pinned context (an evaluated
// request) that would leave the queue with two tag keys differing only by
// case, Key being one the request adds.
type TagKeyCaseError struct {
	Key string
}

func (e *TagKeyCaseError) Error() string {
	return "queues: tag key " + e.Key + " differs only by case from another tag key"
}

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

// PolicyPrincipals binds the principals of a queue policy when it is
// stored and shows them when it is read, as identity's principals client
// does: Bind replaces each IAM user and role ARN with the principal's
// unique ID, and Show turns each unique ID a user or role still has back
// into its ARN.
type PolicyPrincipals interface {
	Bind(ctx context.Context, doc string) (string, error)
	Show(ctx context.Context, doc string) (string, error)
}

// BindFunc returns the document to store for a queue policy that passed
// CheckPolicy, or an error to refuse it with.
type BindFunc func(ctx context.Context, doc string) (string, error)

// QueueAccess is what authorizing a request on a queue reads of it, read
// together: the ID the request is pinned to (WithQueuePin), the tags for
// aws:ResourceTag/<key>, and the queue's Policy attribute, "" when it has
// none, so the policy evaluated is the one of the queue acted on.
type QueueAccess struct {
	Tags   map[string]string
	Policy string
	ID     QueueID
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

// The statuses of a message move task, as ListMessageMoveTasks reports
// them.
const (
	MoveTaskRunning    = "RUNNING"
	MoveTaskCompleted  = "COMPLETED"
	MoveTaskCancelling = "CANCELLING" //nolint:misspell // SQS's spelling
	MoveTaskCancelled  = "CANCELLED"  //nolint:misspell // SQS's spelling
	MoveTaskFailed     = "FAILED"
)

// MoveScope is what a message move task started on a queue now would move:
// its messages up to the row UpTo, and the names of the queues those
// messages were moved to it from (their DeadLetterQueueSourceArn), which a
// task without a destination sends them back to.
type MoveScope struct {
	Origins []string
	UpTo    int64
}

// StartMoveInput is a message move task to start.
type StartMoveInput struct {
	// MaxPerSecond is nil for the system-optimized rate.
	MaxPerSecond *int
	Source       string
	// Destination is the name of the queue to move the messages to, or ""
	// to move each back to the queue it came from.
	Destination string
	// UpTo is the MoveScope the request was evaluated on: the task moves
	// none of the source's messages past it.
	UpTo int64
}

// MoveTask is a message move task as ListMessageMoveTasks reports it.
type MoveTask struct {
	StartedAt time.Time
	// MaxPerSecond is nil when the task was started without one.
	MaxPerSecond *int
	// Handle is the task's handle; ListMessageMoveTasks shows it only
	// while the task runs.
	Handle    string
	Status    string
	SourceARN string
	// DestinationARN is "" for a task that moves each message back to the
	// queue it came from.
	DestinationARN string
	// FailureReason is set on a FAILED task.
	FailureReason string
	Moved         int64
	ToMove        int64
}

// taskHandle is what a task handle carries, as SQS's carry it: the task's
// ID and its source queue's ARN, as JSON in base64 (the TaskHandle of the
// API reference's StartMessageMoveTask example decodes to this form).
type taskHandle struct {
	TaskID    string `json:"taskId"`
	SourceARN string `json:"sourceArn"`
}

// TaskHandle returns the handle of the task id on the queue of sourceARN.
func TaskHandle(id, sourceARN string) string {
	raw, err := json.Marshal(taskHandle{TaskID: id, SourceARN: sourceARN})
	if err != nil {
		// Two strings always encode.
		panic("contracts: encode a task handle: " + err.Error())
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// TaskHandleSource returns the source queue's ARN a task handle carries,
// or false for a string that is no task handle.
func TaskHandleSource(handle string) (string, bool) {
	raw, err := base64.StdEncoding.DecodeString(handle)
	if err != nil {
		return "", false
	}
	var h taskHandle
	if json.Unmarshal(raw, &h) != nil || h.TaskID == "" || h.SourceARN == "" {
		return "", false
	}
	return h.SourceARN, true
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
	// QueueAccess returns what authorizing a request on the queue reads
	// of it, read together.
	QueueAccess(ctx context.Context, name string) (*QueueAccess, error)
	// ListDeadLetterSourceQueues lists the names of the queues whose
	// RedrivePolicy targets the queue.
	ListDeadLetterSourceQueues(ctx context.Context, name string,
		page PageInput) (*Page[string], error)
	// AddPermission adds the statement p generates to the queue's Policy,
	// as bind returns that statement alone; nil stores it as generated.
	AddPermission(ctx context.Context, name string, p Permission, bind BindFunc) error
	// RemovePermission removes a statement, binding nothing.
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

	// MoveScope returns what a message move task started on the queue now
	// would move.
	MoveScope(ctx context.Context, name string) (*MoveScope, error)
	// StartMoveTask starts a message move task in the background and
	// returns its handle.
	StartMoveTask(ctx context.Context, in StartMoveInput) (string, error)
	// ListMoveTasks returns up to n of the queue's most recent message move
	// tasks, the oldest first.
	ListMoveTasks(ctx context.Context, name string, n int) ([]*MoveTask, error)
	// CancelMoveTask cancels the running task of the handle on the queue
	// name, and returns how many messages it moved.
	CancelMoveTask(ctx context.Context, name, handle string) (int64, error)

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
