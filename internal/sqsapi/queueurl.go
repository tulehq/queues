package sqsapi

// The requests that name their queue by QueueUrl, each answering Queue, so
// one function can read the queue of any of them.

// Queue returns the request's QueueUrl.
func (r *QueueRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *ListDeadLetterSourceQueuesRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *GetQueueAttributesRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *SetQueueAttributesRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *TagQueueRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *UntagQueueRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *AddPermissionRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *RemovePermissionRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *SendMessageRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *SendMessageBatchRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *ReceiveMessageRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *DeleteMessageRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *DeleteMessageBatchRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *ChangeMessageVisibilityRequest) Queue() *string { return r.QueueURL }

// Queue returns the request's QueueUrl.
func (r *ChangeMessageVisibilityBatchRequest) Queue() *string { return r.QueueURL }
