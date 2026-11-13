package queues

import (
	"slices"

	"tule.dev/sigv4"

	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

// target returns what a send to the queue is checked against.
func target(q *contracts.Queue) (sqsapi.SendTarget, error) {
	set, err := contracts.SettingsOf(q.Attributes)
	if err != nil {
		return sqsapi.SendTarget{}, err //nolint:wrapcheck // it names the attribute
	}
	return sqsapi.SendTarget{
		FIFO: q.FIFO, ContentBasedDeduplication: set.ContentBasedDeduplication,
		MaximumMessageSize: set.MaximumMessageSize,
	}, nil
}

// senderID is the SenderId of the messages a request sends: the access key
// ID that signed it.
func senderID(c *call) string {
	p, _ := sigv4.PrincipalFrom(c.Request().Context())
	return p.AccessKeyID
}

func sendResult(in *contracts.SendInput, res contracts.SendResult) sqsapi.SendMessageResult {
	return sqsapi.SendMessageResult{
		MessageID:                    res.MessageID,
		MD5OfMessageBody:             in.MD5OfBody,
		MD5OfMessageAttributes:       in.MD5OfAttributes,
		MD5OfMessageSystemAttributes: in.MD5OfSystemAttributes,
		SequenceNumber:               res.SequenceNumber,
	}
}

func (m *Module) sendMessage(c *call) (any, error) {
	req, err := decoded[sqsapi.SendMessageRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	q, err := m.store.QueueInfo(c, name)
	if err != nil {
		return nil, err
	}
	t, err := target(q)
	if err != nil {
		return nil, err
	}
	in, _, err := sqsapi.CheckSend(sqsapi.SendFields{
		Body: req.MessageBody, DelaySeconds: req.DelaySeconds,
		Attributes: req.MessageAttributes, SystemAttributes: req.MessageSystemAttributes,
		DeduplicationID: req.MessageDeduplicationID, GroupID: req.MessageGroupID,
	}, t)
	if err != nil {
		return nil, err
	}
	in.SenderID = senderID(c)
	res, err := m.store.SendMessages(c, name, []contracts.SendInput{in})
	if err != nil {
		return nil, err
	}
	return sendResult(&in, res[0]), nil
}

// sendMessageBatch sends the entries that pass their checks, in order, and
// reports each failed entry with its error. Entries that fail their checks
// do not stop the rest.
func (m *Module) sendMessageBatch(c *call) (any, error) {
	req, err := decoded[sqsapi.SendMessageBatchRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckBatchIDs(idsOf(req.Entries,
		func(e sqsapi.SendMessageBatchRequestEntry) *string { return e.ID })); err != nil {
		return nil, err
	}
	q, err := m.store.QueueInfo(c, name)
	if err != nil {
		return nil, err
	}
	t, err := target(q)
	if err != nil {
		return nil, err
	}
	out := sqsapi.SendMessageBatchResult{
		Successful: []sqsapi.SendMessageBatchResultEntry{},
		Failed:     []sqsapi.BatchResultErrorEntry{},
	}
	sends, sentIDs, total := checkSendEntries(c, req.Entries, t, &out)
	// The batch's size is that of the messages it sends: their bodies and
	// attributes.
	if err = sqsapi.CheckBatchSize(total); err != nil {
		return nil, err
	}
	if len(sends) == 0 {
		return out, nil
	}
	res, err := m.store.SendMessages(c, name, sends)
	if err != nil {
		return nil, err
	}
	for i := range sends {
		out.Successful = append(out.Successful, sqsapi.SendMessageBatchResultEntry{
			ID: sentIDs[i], SendMessageResult: sendResult(&sends[i], res[i]),
		})
	}
	return out, nil
}

// checkSendEntries checks a batch's entries against the queue, reporting
// those that fail in out, and returns the rest for the store with their IDs
// and their total size.
func checkSendEntries(c *call, entries []sqsapi.SendMessageBatchRequestEntry,
	t sqsapi.SendTarget, out *sqsapi.SendMessageBatchResult,
) ([]contracts.SendInput, []string, int) {
	var (
		sends   []contracts.SendInput
		sentIDs []string
		total   int
	)
	sender := senderID(c)
	for _, e := range entries {
		in, size, err := sqsapi.CheckSend(sqsapi.SendFields{
			Body: e.MessageBody, DelaySeconds: e.DelaySeconds,
			Attributes: e.MessageAttributes, SystemAttributes: e.MessageSystemAttributes,
			DeduplicationID: e.MessageDeduplicationID, GroupID: e.MessageGroupID,
		}, t)
		if err != nil {
			out.Failed = append(out.Failed, entryError(c, *e.ID, err))
			continue
		}
		in.SenderID = sender
		total += size
		sends = append(sends, in)
		sentIDs = append(sentIDs, *e.ID)
	}
	return sends, sentIDs, total
}

// idsOf returns the IDs of a batch's entries.
func idsOf[E any](entries []E, id func(E) *string) []*string {
	ids := make([]*string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, id(e))
	}
	return ids
}

// batchErrors reports the store's per-entry errors of a batch: each nil one
// as a success, the rest as failures.
func batchErrors(c *call, ids []string, errs []error, successful *[]sqsapi.BatchResultEntry,
	failed *[]sqsapi.BatchResultErrorEntry,
) {
	for i, err := range errs {
		if err != nil {
			*failed = append(*failed, entryError(c, ids[i], err))
			continue
		}
		*successful = append(*successful, sqsapi.BatchResultEntry{ID: ids[i]})
	}
}

func (m *Module) receiveMessage(c *call) (any, error) {
	req, err := decoded[sqsapi.ReceiveMessageRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckReceive(req); err != nil {
		return nil, err
	}
	systemNames := slices.Concat(req.AttributeNames, req.MessageSystemAttributeNames)
	if err = sqsapi.CheckSystemAttributeNames(systemNames); err != nil {
		return nil, err
	}
	in := contracts.ReceiveInput{
		Max: 1, VisibilityTimeout: req.VisibilityTimeout, WaitSeconds: req.WaitTimeSeconds,
	}
	if req.MaxNumberOfMessages != nil {
		in.Max = *req.MaxNumberOfMessages
	}
	if req.ReceiveRequestAttemptID != nil {
		in.AttemptID = *req.ReceiveRequestAttemptID
	}
	msgs, err := m.store.ReceiveMessages(c, name, in)
	if err != nil {
		return nil, err
	}
	out := sqsapi.ReceiveMessageResult{}
	for _, msg := range msgs {
		out.Messages = append(out.Messages,
			sqsapi.Received(msg, systemNames, req.MessageAttributeNames))
	}
	return out, nil
}

func (m *Module) deleteMessage(c *call) (any, error) {
	req, err := decoded[sqsapi.DeleteMessageRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	handle, err := sqsapi.Required("ReceiptHandle", req.ReceiptHandle)
	if err != nil {
		return nil, err
	}
	errs, err := m.store.DeleteMessages(c, name, []string{handle})
	if err != nil {
		return nil, err
	}
	return nil, errs[0]
}

func (m *Module) deleteMessageBatch(c *call) (any, error) {
	req, err := decoded[sqsapi.DeleteMessageBatchRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckBatchIDs(idsOf(req.Entries,
		func(e sqsapi.DeleteMessageBatchRequestEntry) *string { return e.ID })); err != nil {
		return nil, err
	}
	out := sqsapi.DeleteMessageBatchResult{
		Successful: []sqsapi.BatchResultEntry{}, Failed: []sqsapi.BatchResultErrorEntry{},
	}
	var handles, sentIDs []string
	for _, e := range req.Entries {
		h, rErr := sqsapi.Required("ReceiptHandle", e.ReceiptHandle)
		if rErr != nil {
			out.Failed = append(out.Failed, entryError(c, *e.ID, rErr))
			continue
		}
		handles = append(handles, h)
		sentIDs = append(sentIDs, *e.ID)
	}
	errs, err := m.store.DeleteMessages(c, name, handles)
	if err != nil {
		return nil, err
	}
	batchErrors(c, sentIDs, errs, &out.Successful, &out.Failed)
	return out, nil
}

func (m *Module) changeMessageVisibility(c *call) (any, error) {
	req, err := decoded[sqsapi.ChangeMessageVisibilityRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	change, err := checkChange(req.ReceiptHandle, req.VisibilityTimeout)
	if err != nil {
		return nil, err
	}
	errs, err := m.store.ChangeVisibility(c, name, []contracts.VisibilityChange{change})
	if err != nil {
		return nil, err
	}
	return nil, errs[0]
}

func (m *Module) changeMessageVisibilityBatch(c *call) (any, error) {
	req, err := decoded[sqsapi.ChangeMessageVisibilityBatchRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckBatchIDs(idsOf(req.Entries,
		func(e sqsapi.ChangeMessageVisibilityBatchRequestEntry) *string {
			return e.ID
		})); err != nil {
		return nil, err
	}
	out := sqsapi.ChangeMessageVisibilityBatchResult{
		Successful: []sqsapi.BatchResultEntry{}, Failed: []sqsapi.BatchResultErrorEntry{},
	}
	var (
		changes []contracts.VisibilityChange
		sentIDs []string
	)
	for _, e := range req.Entries {
		change, cErr := checkChange(e.ReceiptHandle, e.VisibilityTimeout)
		if cErr != nil {
			out.Failed = append(out.Failed, entryError(c, *e.ID, cErr))
			continue
		}
		changes = append(changes, change)
		sentIDs = append(sentIDs, *e.ID)
	}
	errs, err := m.store.ChangeVisibility(c, name, changes)
	if err != nil {
		return nil, err
	}
	batchErrors(c, sentIDs, errs, &out.Successful, &out.Failed)
	return out, nil
}

// checkChange checks a ChangeMessageVisibility's handle and timeout, both
// required.
func checkChange(handle *string, timeout *int) (contracts.VisibilityChange, error) {
	h, err := sqsapi.Required("ReceiptHandle", handle)
	if err != nil {
		return contracts.VisibilityChange{}, err
	}
	t, err := sqsapi.CheckVisibilityTimeout(timeout)
	if err != nil {
		return contracts.VisibilityChange{}, err
	}
	return contracts.VisibilityChange{ReceiptHandle: h, Timeout: t}, nil
}
