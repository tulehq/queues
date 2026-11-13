package queues

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"tule.dev/policy"
	"tule.dev/sigv4"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

// operationAccess is how each routed operation is authorized: the IAM
// action and the resource the SQS Service Authorization Reference gives it
// (servicereference.us-east-1.amazonaws.com/v1/sqs/sqs.json, whose
// Operations list the actions each API operation is authorized as). Every
// operation but ListQueues acts on the queue resource,
// arn:aws:sqs:<region>:<account>:<name>; ListQueues has no resource type
// and is evaluated on "*". The batch operations are their base actions on
// their one queue, so a batch is allowed or denied whole: every entry names
// the same queue, and SQS has no per-entry AccessDenied.
var operationAccess = map[string]access{
	"AddPermission": {
		action: "sqs:AddPermission",
		target: byQueueURL[sqsapi.AddPermissionRequest],
	},
	"ChangeMessageVisibility": {
		action: "sqs:ChangeMessageVisibility",
		target: byQueueURL[sqsapi.ChangeMessageVisibilityRequest],
	},
	"ChangeMessageVisibilityBatch": {
		action: "sqs:ChangeMessageVisibility",
		target: byQueueURL[sqsapi.ChangeMessageVisibilityBatchRequest],
	},
	"CreateQueue": {action: "sqs:CreateQueue", target: createQueueTarget},
	"DeleteMessage": {
		action: "sqs:DeleteMessage",
		target: byQueueURL[sqsapi.DeleteMessageRequest],
	},
	"DeleteMessageBatch": {
		action: "sqs:DeleteMessage",
		target: byQueueURL[sqsapi.DeleteMessageBatchRequest],
	},
	"DeleteQueue": {
		action: "sqs:DeleteQueue",
		target: byQueueURL[sqsapi.QueueRequest],
	},
	"GetQueueAttributes": {
		action: "sqs:GetQueueAttributes",
		target: byQueueURL[sqsapi.GetQueueAttributesRequest],
	},
	"GetQueueUrl": {action: "sqs:GetQueueUrl", target: getQueueURLTarget},
	"ListDeadLetterSourceQueues": {
		action: "sqs:ListDeadLetterSourceQueues",
		target: byQueueURL[sqsapi.ListDeadLetterSourceQueuesRequest],
	},
	"ListQueueTags": {
		action: "sqs:ListQueueTags",
		target: byQueueURL[sqsapi.QueueRequest],
	},
	"ListQueues": {action: "sqs:ListQueues", target: anyResource},
	"PurgeQueue": {
		action: "sqs:PurgeQueue",
		target: byQueueURL[sqsapi.QueueRequest],
	},
	"ReceiveMessage": {
		action: "sqs:ReceiveMessage",
		target: byQueueURL[sqsapi.ReceiveMessageRequest],
	},
	"RemovePermission": {
		action: "sqs:RemovePermission",
		target: byQueueURL[sqsapi.RemovePermissionRequest],
	},
	"SendMessage": {
		action: "sqs:SendMessage",
		target: byQueueURL[sqsapi.SendMessageRequest],
	},
	"SendMessageBatch": {
		action: "sqs:SendMessage",
		target: byQueueURL[sqsapi.SendMessageBatchRequest],
	},
	"SetQueueAttributes": {
		action: "sqs:SetQueueAttributes",
		target: byQueueURL[sqsapi.SetQueueAttributesRequest],
	},
	"TagQueue":   {action: "sqs:TagQueue", target: tagQueueTarget},
	"UntagQueue": {action: "sqs:UntagQueue", target: untagQueueTarget},
}

// ownerOnly are the operations "Cross-account permissions don't apply to"
// (SQS Developer Guide, "Basic examples of Amazon SQS policies", Example
// 4, and each operation's API reference): a queue policy grants them to
// principals of the queue's own account only. A principal of another
// account is evaluated on them without the queue's policy, so nothing
// grants it them.
var ownerOnly = map[string]bool{
	"AddPermission": true, "CreateQueue": true, "DeleteQueue": true, "ListQueues": true,
	"ListQueueTags": true, "RemovePermission": true, "SetQueueAttributes": true,
	"TagQueue": true, "UntagQueue": true,
}

// tagQueueAction is the action CreateQueue also needs when it tags the
// queue it creates: "To be able to tag a queue on creation, you must have
// the sqs:CreateQueue and sqs:TagQueue permissions" (SQS API Reference,
// CreateQueue), and the reference authorizes CreateQueue as both.
const tagQueueAction = "sqs:TagQueue"

// maxTagKeyLen is the longest tag key SQS takes (SQS Developer Guide, tag
// quotas), which bounds the aws:TagKeys values of an UntagQueue.
const maxTagKeyLen = 128

// access is one operation's entry: its action, and how to find the
// resource and the request's own context keys.
type access struct {
	target func(*Module, *call) (accessTarget, error)
	action string
}

// accessTarget is what one request acts on.
type accessTarget struct {
	// context holds the request's own keys: aws:RequestTag/<key> and
	// aws:TagKeys.
	context map[string][]string
	// queue is the queue the request names, or "" for "*".
	queue string
	// tagKeys are the keys a TagQueue adds, checked against the stored
	// ones.
	tagKeys []string
	// alsoTag is set for a CreateQueue that tags its queue.
	alsoTag bool
	// exists is set when the queue is expected to exist, so its tags
	// supply aws:ResourceTag/<key>.
	exists bool
}

// authorize checks the request against its principal's identity policies
// and the queue's policy before the operation runs. A root principal is
// not evaluated, and its request is not read here. A request is decoded
// once, as its operation's own type (decoded), and the operation acts on
// that same decoding, so a missing or malformed queue URL fails as the
// operation would fail and the values evaluated are the values acted on.
func (m *Module) authorize(c *call) error {
	p, ok := sigv4.PrincipalFrom(c.Request().Context())
	if !ok {
		return errors.New("authorize: the request carries no verified principal")
	}
	if m.authorizer.IsRoot(p) {
		return nil
	}
	a, ok := operationAccess[c.op]
	if !ok {
		return fmt.Errorf("authorize: no access entry for %s", c.op)
	}
	t, err := a.target(m, c)
	if err != nil {
		return err
	}
	// The resource is rebuilt from the configured partition, region and
	// account and the queue's name, never taken from the request: a queue
	// URL names its queue by path, and one of another account is answered
	// QueueDoesNotExist by queueName before this point.
	resource := "*"
	if t.queue != "" {
		resource = m.settings.ARN(t.queue)
	}
	ctx := t.context
	var doc string
	// The tags are read whatever the policies say: deciding from a
	// policy's bytes whether it asks for them would differ from the parser,
	// which reads escaped key names.
	if t.exists {
		if ctx, doc, err = m.readQueue(c, t, ctx); err != nil {
			return err
		}
	}
	pol, err := m.queuePolicy(c.op, p, t.queue, doc)
	if err != nil {
		return err
	}
	checks := []authz.Check{{Action: a.action, Resource: resource, Context: ctx, Policy: pol}}
	if t.alsoTag {
		checks = append(checks, authz.Check{
			Action: tagQueueAction, Resource: resource, Context: ctx, Policy: pol,
		})
	}
	return m.authorizer.Authorize(
		c.Request(),
		p,
		checks...) //nolint:wrapcheck // an *authz.Denied, or a fault ToError logs
}

// queuePolicy parses the queue's stored Policy, doc, for an evaluation of
// op by p: none for a principal of another account on an owner-only
// operation. A stored policy that cannot be evaluated fails the request
// (fail closed).
func (m *Module) queuePolicy(
	op string, p sigv4.Principal, queue, doc string,
) (*policy.Policy, error) {
	if ownerOnly[op] && p.AccountID != m.settings.AccountID {
		return nil, nil
	}
	pol, err := m.authorizer.ResourcePolicy(doc)
	if err != nil {
		return nil, fmt.Errorf("authorize: queue %s: %w", queue, err)
	}
	return pol, nil
}

// readQueue adds aws:ResourceTag/<key> for each of the queue's tags to
// ctx, and returns the queue's Policy, both read together with the queue's
// ID. A queue that does not exist has neither; the operation then fails as
// it would have, once allowed.
//
// It pins the call to the queue it read (contracts.WithQueuePin), so the
// operation acts on that queue or, when it was deleted and created again
// under its name since, fails with QueueDoesNotExist; a queue that did not
// exist stays not found. The policy evaluated is therefore the one stored
// for the queue acted on. A tag or the policy changed between this read
// and the operation is not noticed.
//
// tule.dev/policy matches key names without regard to case, so tags that
// differ only by case would feed one aws:ResourceTag key. A request that
// would add a key differing only by case from a stored one is refused as a
// repeated key, and a queue that already holds such keys (a root user
// wrote them) is not evaluated at all.
func (m *Module) readQueue(
	c *call, t accessTarget, ctx map[string][]string,
) (map[string][]string, string, error) {
	q, err := m.store.QueueAccess(c, t.queue)
	if errors.Is(err, contracts.ErrQueueNotFound) {
		c.pinned = contracts.WithQueuePin(c.Context, t.queue, 0)
		return ctx, "", nil
	}
	if err != nil {
		return nil, "", err //nolint:wrapcheck // the store's error, logged by ToError
	}
	c.pinned = contracts.WithQueuePin(c.Context, t.queue, q.ID)
	stored := make(map[string]string, len(q.Tags))
	for k := range q.Tags {
		lower := strings.ToLower(k)
		if other, dup := stored[lower]; dup {
			return nil, "", fmt.Errorf("authorize: queue %s holds tag keys %q and %q, "+
				"which differ only by case", t.queue, other, k)
		}
		stored[lower] = k
	}
	for _, k := range t.tagKeys {
		if s, ok := stored[strings.ToLower(k)]; ok && s != k {
			return nil, "", sqsapi.ErrTagKeyCaseCollision(k)
		}
	}
	out := make(map[string][]string, len(ctx)+len(q.Tags))
	for k, v := range ctx {
		out[k] = v
	}
	for k, v := range q.Tags {
		out["aws:ResourceTag/"+k] = []string{v}
	}
	return out, q.Policy, nil
}

// queueRequest is a request of type T that names its queue by QueueUrl.
type queueRequest[T any] interface {
	*T
	Queue() *string
}

// byQueueURL is the target of an operation whose request, T, names its
// queue by QueueUrl. It decodes the request as the operation's own type, so
// the operation acts on the decoding evaluated.
func byQueueURL[T any, P queueRequest[T]](m *Module, c *call) (accessTarget, error) {
	req, err := decoded[T](c)
	if err != nil {
		return accessTarget{}, err
	}
	name, err := m.queueName(c, P(req).Queue())
	return accessTarget{queue: name, exists: true}, err
}

func anyResource(*Module, *call) (accessTarget, error) {
	return accessTarget{}, nil
}

func getQueueURLTarget(m *Module, c *call) (accessTarget, error) {
	req, err := decoded[sqsapi.GetQueueURLRequest](c)
	if err != nil {
		return accessTarget{}, err
	}
	name, err := sqsapi.Required("QueueName", req.QueueName)
	if err != nil {
		return accessTarget{}, err //nolint:wrapcheck // an *sqsapi.Error
	}
	// A queue of another account is none of this service's: it is answered
	// as the operation answers it, before evaluation.
	if o := req.QueueOwnerAWSAccountID; o != nil && *o != "" && *o != m.settings.AccountID {
		return accessTarget{}, sqsapi.ErrQueueDoesNotExist()
	}
	if _, err = sqsapi.CheckQueueName(name); err != nil {
		return accessTarget{}, err //nolint:wrapcheck // an *sqsapi.Error
	}
	return accessTarget{queue: name, exists: true}, nil
}

func createQueueTarget(_ *Module, c *call) (accessTarget, error) {
	req, err := decoded[sqsapi.CreateQueueRequest](c)
	if err != nil {
		return accessTarget{}, err
	}
	if req.QueueName == nil {
		return accessTarget{}, sqsapi.ErrMissingParameter("QueueName")
	}
	if _, err = sqsapi.CheckQueueName(*req.QueueName); err != nil {
		return accessTarget{}, err //nolint:wrapcheck // an *sqsapi.Error
	}
	ctx, err := requestTags(req.Tags)
	if err != nil {
		return accessTarget{}, err
	}
	return accessTarget{queue: *req.QueueName, context: ctx, alsoTag: len(req.Tags) > 0}, nil
}

func tagQueueTarget(m *Module, c *call) (accessTarget, error) {
	req, err := decoded[sqsapi.TagQueueRequest](c)
	if err != nil {
		return accessTarget{}, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return accessTarget{}, err
	}
	ctx, err := requestTags(req.Tags)
	if err != nil {
		return accessTarget{}, err
	}
	keys := make([]string, 0, len(req.Tags))
	for k := range req.Tags {
		keys = append(keys, k)
	}
	return accessTarget{queue: name, context: ctx, exists: true, tagKeys: keys}, nil
}

func untagQueueTarget(m *Module, c *call) (accessTarget, error) {
	req, err := decoded[sqsapi.UntagQueueRequest](c)
	if err != nil {
		return accessTarget{}, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return accessTarget{}, err
	}
	if err = checkContextValues("TagKeys", len(req.TagKeys)); err != nil {
		return accessTarget{}, err
	}
	for _, k := range req.TagKeys {
		if utf8.RuneCountInString(k) > maxTagKeyLen {
			return accessTarget{}, sqsapi.ErrInvalidParameterValue(fmt.Sprintf(
				"Tag key %q is longer than %d characters.", k, maxTagKeyLen))
		}
	}
	var ctx map[string][]string
	if len(req.TagKeys) > 0 {
		ctx = map[string][]string{"aws:TagKeys": req.TagKeys}
	}
	return accessTarget{queue: name, context: ctx, exists: true}, nil
}

// requestTags returns aws:RequestTag/<key> for each tag and aws:TagKeys
// for all, checked as the operation checks them first, so their lengths
// are bounded.
func requestTags(tags map[string]string) (map[string][]string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	if err := checkContextValues("Tags", len(tags)); err != nil {
		return nil, err
	}
	if err := sqsapi.CheckTags(tags); err != nil {
		return nil, err //nolint:wrapcheck // an *sqsapi.Error
	}
	// Keys that differ only by case would feed one aws:RequestTag key.
	seen := make(map[string]bool, len(tags))
	for k := range tags {
		lower := strings.ToLower(k)
		if seen[lower] {
			return nil, sqsapi.ErrTagKeyCaseCollision(k)
		}
		seen[lower] = true
	}
	ctx := make(map[string][]string, len(tags)+1)
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		ctx["aws:RequestTag/"+k] = []string{v}
		keys = append(keys, k)
	}
	ctx["aws:TagKeys"] = keys
	return ctx, nil
}

// checkContextValues refuses, for an evaluated principal, a request with
// more tags or tag keys than authz evaluates.
func checkContextValues(field string, n int) error {
	if n <= authz.MaxContextValues {
		return nil
	}
	return sqsapi.ErrInvalidParameterValue(fmt.Sprintf(
		"The request has %d %s; a principal evaluated against its policies may send "+
			"at most %d.", n, field, authz.MaxContextValues))
}
