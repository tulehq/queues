package queues

import (
	"errors"

	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

// The message move task operations name queues by ARN, and answer a queue
// or task that does not exist with ResourceNotFoundException, the error
// their API references list, rather than QueueDoesNotExist.

// errNoQueue is the error for an ARN that names no queue.
func errNoQueue() error {
	return sqsapi.ErrResourceNotFound("The specified queue does not exist.")
}

// errNoTask is the error for a task handle that names no task.
func errNoTask() error {
	return sqsapi.ErrResourceNotFound("The task handle names no message move task.")
}

// moveErr is the error a move task operation answers for the store's err.
func moveErr(err error) error {
	if errors.Is(err, contracts.ErrQueueNotFound) {
		return errNoQueue()
	}
	return err
}

// queueOfARN returns the name of the queue a required ARN parameter names.
// A value that is no SQS ARN fails with InvalidParameterValue. An ARN of
// another account or region, or one whose name no queue can have, names no
// queue of this service: it fails with ResourceNotFoundException before the
// request is evaluated, as a queue URL of another account does.
func (m *Module) queueOfARN(field string, arn *string) (string, error) {
	v, err := sqsapi.Required(field, arn)
	if err != nil {
		return "", err
	}
	if err = sqsapi.CheckQueueARN(field, v); err != nil {
		return "", err //nolint:wrapcheck // an *sqsapi.Error
	}
	name, ok := m.settings.NameOfARN(v)
	if !ok {
		return "", errNoQueue()
	}
	if _, err = sqsapi.CheckQueueName(name); err != nil {
		return "", errNoQueue()
	}
	return name, nil
}

// destinationOf returns the name of the queue a StartMessageMoveTask's
// DestinationArn names, or "" when it is left out or blank: "If this field
// is left blank, the messages will be redriven back to their respective
// original source queues" (API reference).
func (m *Module) destinationOf(req *sqsapi.StartMessageMoveTaskRequest) (string, error) {
	if req.DestinationARN == nil || *req.DestinationARN == "" {
		return "", nil
	}
	return m.queueOfARN("DestinationArn", req.DestinationARN)
}

// moveScope returns what a task started on the queue name would move, read
// once per request: authorization evaluates the send to each queue its
// messages came from, and the task moves those messages and no later one.
func (m *Module) moveScope(c *call, name string) (*contracts.MoveScope, error) {
	if c.scope == nil {
		s, err := m.store.MoveScope(c, name)
		if err != nil {
			return nil, err //nolint:wrapcheck // the store's error, mapped by moveErr
		}
		c.scope = s
	}
	return c.scope, nil
}

// taskSource returns the name of the source queue a task handle names.
func (m *Module) taskSource(handle *string) (string, string, error) {
	h, err := sqsapi.Required("TaskHandle", handle)
	if err != nil {
		return "", "", err
	}
	arn, ok := contracts.TaskHandleSource(h)
	if !ok {
		return "", "", errNoTask()
	}
	name, ok := m.settings.NameOfARN(arn)
	if !ok {
		return "", "", errNoTask()
	}
	if _, err = sqsapi.CheckQueueName(name); err != nil {
		return "", "", errNoTask()
	}
	return name, h, nil
}

func (m *Module) startMessageMoveTask(c *call) (any, error) {
	req, err := decoded[sqsapi.StartMessageMoveTaskRequest](c)
	if err != nil {
		return nil, err
	}
	source, err := m.queueOfARN("SourceArn", req.SourceARN)
	if err != nil {
		return nil, err
	}
	destination, err := m.destinationOf(req)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckMoveRate(req.MaxNumberOfMessagesPerSecond); err != nil {
		return nil, err
	}
	scope, err := m.moveScope(c, source)
	if err != nil {
		return nil, moveErr(err)
	}
	handle, err := m.store.StartMoveTask(c, contracts.StartMoveInput{
		Source: source, Destination: destination,
		MaxPerSecond: req.MaxNumberOfMessagesPerSecond, UpTo: scope.UpTo,
	})
	if err != nil {
		return nil, moveErr(err)
	}
	return sqsapi.StartMessageMoveTaskResult{TaskHandle: handle}, nil
}

func (m *Module) listMessageMoveTasks(c *call) (any, error) {
	req, err := decoded[sqsapi.ListMessageMoveTasksRequest](c)
	if err != nil {
		return nil, err
	}
	source, err := m.queueOfARN("SourceArn", req.SourceARN)
	if err != nil {
		return nil, err
	}
	n, err := sqsapi.CheckMoveTaskResults(req.MaxResults)
	if err != nil {
		return nil, err
	}
	tasks, err := m.store.ListMoveTasks(c, source, n)
	if err != nil {
		return nil, moveErr(err)
	}
	out := sqsapi.ListMessageMoveTasksResult{}
	for _, t := range tasks {
		e := sqsapi.ListMessageMoveTasksResultEntry{
			Status: t.Status, SourceARN: t.SourceARN, DestinationARN: t.DestinationARN,
			MaxNumberOfMessagesPerSecond:      t.MaxPerSecond,
			ApproximateNumberOfMessagesMoved:  t.Moved,
			ApproximateNumberOfMessagesToMove: &t.ToMove,
			FailureReason:                     t.FailureReason,
			StartedTimestamp:                  t.StartedAt.UnixMilli(),
		}
		// "When this field is returned in the response of the
		// ListMessageMoveTasks action, it is only populated for tasks that
		// are in RUNNING status" (ListMessageMoveTasksResultEntry).
		if t.Status == contracts.MoveTaskRunning {
			e.TaskHandle = t.Handle
		}
		out.Results = append(out.Results, e)
	}
	return out, nil
}

func (m *Module) cancelMessageMoveTask(c *call) (any, error) {
	req, err := decoded[sqsapi.CancelMessageMoveTaskRequest](c)
	if err != nil {
		return nil, err
	}
	source, handle, err := m.taskSource(req.TaskHandle)
	if err != nil {
		return nil, err
	}
	moved, err := m.store.CancelMoveTask(c, source, handle)
	if err != nil {
		if errors.Is(err, contracts.ErrQueueNotFound) {
			return nil, errNoTask()
		}
		return nil, err //nolint:wrapcheck // the store's error, mapped by ToError
	}
	return sqsapi.CancelMessageMoveTaskResult{ApproximateNumberOfMessagesMoved: moved}, nil
}
