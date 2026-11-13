package queues

import (
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

func (m *Module) createQueue(c *call) (any, error) {
	req, err := decoded[sqsapi.CreateQueueRequest](c)
	if err != nil {
		return nil, err
	}
	if req.QueueName == nil {
		return nil, sqsapi.ErrMissingParameter("QueueName")
	}
	name := *req.QueueName
	attrs, fifo, err := sqsapi.CheckCreateAttributes(name, req.Attributes)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckTags(req.Tags); err != nil {
		return nil, err
	}
	q, err := m.store.CreateQueue(c, contracts.CreateQueueInput{
		Name: name, FIFO: fifo, Attributes: attrs, Tags: req.Tags,
	})
	if err != nil {
		return nil, err
	}
	return sqsapi.QueueURLResult{QueueURL: m.queueURL(c, q.Name)}, nil
}

func (m *Module) getQueueURL(c *call) (any, error) {
	req, err := decoded[sqsapi.GetQueueURLRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := sqsapi.Required("QueueName", req.QueueName)
	if err != nil {
		return nil, err
	}
	if o := req.QueueOwnerAWSAccountID; o != nil && *o != "" && *o != m.settings.AccountID {
		return nil, sqsapi.ErrQueueDoesNotExist()
	}
	q, err := m.store.QueueInfo(c, name)
	if err != nil {
		return nil, err
	}
	return sqsapi.QueueURLResult{QueueURL: m.queueURL(c, q.Name)}, nil
}

// queueOnly decodes a request that takes nothing but a queue, and returns
// the queue's name.
func (m *Module) queueOnly(c *call) (string, error) {
	req, err := decoded[sqsapi.QueueRequest](c)
	if err != nil {
		return "", err
	}
	return m.queueName(c, req.QueueURL)
}

func (m *Module) deleteQueue(c *call) (any, error) {
	name, err := m.queueOnly(c)
	if err != nil {
		return nil, err
	}
	return nil, m.store.DeleteQueue(c, name)
}

func (m *Module) purgeQueue(c *call) (any, error) {
	name, err := m.queueOnly(c)
	if err != nil {
		return nil, err
	}
	return nil, m.store.PurgeQueue(c, name)
}

func (m *Module) listQueues(c *call) (any, error) {
	req, err := decoded[sqsapi.ListQueuesRequest](c)
	if err != nil {
		return nil, err
	}
	n, err := sqsapi.CheckMaxResults(req.MaxResults)
	if err != nil {
		return nil, err
	}
	var prefix, token string
	if req.QueueNamePrefix != nil {
		prefix = *req.QueueNamePrefix
	}
	if req.NextToken != nil {
		token = *req.NextToken
	}
	page, err := m.store.ListQueues(c, prefix, contracts.PageInput{
		NextToken:  token,
		MaxResults: n,
	})
	if err != nil {
		return nil, err
	}
	return sqsapi.ListQueuesResult{
		QueueURLs: m.queueURLs(c, page.Items), NextToken: page.NextToken,
	}, nil
}

func (m *Module) listDeadLetterSourceQueues(c *call) (any, error) {
	req, err := decoded[sqsapi.ListDeadLetterSourceQueuesRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	n, err := sqsapi.CheckMaxResults(req.MaxResults)
	if err != nil {
		return nil, err
	}
	var token string
	if req.NextToken != nil {
		token = *req.NextToken
	}
	page, err := m.store.ListDeadLetterSourceQueues(c, name,
		contracts.PageInput{NextToken: token, MaxResults: n})
	if err != nil {
		return nil, err
	}
	return sqsapi.ListDeadLetterSourceQueuesResult{
		QueueURLs: m.queueURLs(c, page.Items), NextToken: page.NextToken,
	}, nil
}

func (m *Module) getQueueAttributes(c *call) (any, error) {
	req, err := decoded[sqsapi.GetQueueAttributesRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	q, err := m.store.GetQueue(c, name)
	if err != nil {
		return nil, err
	}
	attrs, err := sqsapi.QueueAttributes(q, m.settings.ARN(q.Name), req.AttributeNames)
	if err != nil {
		return nil, err
	}
	return sqsapi.GetQueueAttributesResult{Attributes: attrs}, nil
}

func (m *Module) setQueueAttributes(c *call) (any, error) {
	req, err := decoded[sqsapi.SetQueueAttributesRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if req.Attributes == nil {
		return nil, sqsapi.ErrMissingParameter("Attributes")
	}
	q, err := m.store.QueueInfo(c, name)
	if err != nil {
		return nil, err
	}
	attrs, err := sqsapi.CheckQueueAttributes(req.Attributes, q.Attributes, q.FIFO)
	if err != nil {
		return nil, err
	}
	return nil, m.store.SetQueueAttributes(c, name, attrs)
}

func (m *Module) tagQueue(c *call) (any, error) {
	req, err := decoded[sqsapi.TagQueueRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if len(req.Tags) == 0 {
		return nil, sqsapi.ErrMissingParameter("Tags")
	}
	if err = sqsapi.CheckTags(req.Tags); err != nil {
		return nil, err
	}
	return nil, m.store.TagQueue(c, name, req.Tags)
}

func (m *Module) untagQueue(c *call) (any, error) {
	req, err := decoded[sqsapi.UntagQueueRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	if len(req.TagKeys) == 0 {
		return nil, sqsapi.ErrMissingParameter("TagKeys")
	}
	return nil, m.store.UntagQueue(c, name, req.TagKeys)
}

func (m *Module) listQueueTags(c *call) (any, error) {
	name, err := m.queueOnly(c)
	if err != nil {
		return nil, err
	}
	tags, err := m.store.ListQueueTags(c, name)
	if err != nil {
		return nil, err
	}
	return sqsapi.ListQueueTagsResult{Tags: tags}, nil
}

func (m *Module) addPermission(c *call) (any, error) {
	req, err := decoded[sqsapi.AddPermissionRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	p, err := sqsapi.CheckPermission(req)
	if err != nil {
		return nil, err
	}
	return nil, m.store.AddPermission(c, name, p)
}

func (m *Module) removePermission(c *call) (any, error) {
	req, err := decoded[sqsapi.RemovePermissionRequest](c)
	if err != nil {
		return nil, err
	}
	name, err := m.queueName(c, req.QueueURL)
	if err != nil {
		return nil, err
	}
	label, err := sqsapi.Required("Label", req.Label)
	if err != nil {
		return nil, err
	}
	if err = sqsapi.CheckLabel(label); err != nil {
		return nil, err
	}
	return nil, m.store.RemovePermission(c, name, label)
}
