package contracts

// QueueSettings name the account and region the queues belong to, which
// their URLs and ARNs carry, and the base URL queue URLs start with when it
// is set.
type QueueSettings struct {
	AccountID string `validate:"required"`
	Region    string `validate:"required"`
	BaseURL   string
}

// ARN returns the ARN of the queue name.
func (q *QueueSettings) ARN(name string) string {
	return "arn:aws:sqs:" + q.Region + ":" + q.AccountID + ":" + name
}

// NameOfARN returns the queue name an ARN of this account and region
// names, or false when it names none.
func (q *QueueSettings) NameOfARN(arn string) (string, bool) {
	prefix := "arn:aws:sqs:" + q.Region + ":" + q.AccountID + ":"
	if len(arn) <= len(prefix) || arn[:len(prefix)] != prefix {
		return "", false
	}
	return arn[len(prefix):], true
}
