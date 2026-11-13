package sqsapi

import (
	"encoding/base64"
	"encoding/xml"
	"slices"
)

// The request and response shapes of the operations in scope, following the
// SQS model (aws-sdk-go-v2/service/sqs). Each one is read from either
// protocol: the JSON tags are the AWS JSON member names, and Query requests
// are decoded into the same structs by the decoders in query.go. The XML
// tags are the Query protocol's element names, the model's xmlName and
// xmlFlattened traits, so one response value is written in either protocol.
// A field the model marks optional is left out when empty.

// Blob is binary data: base64 in both protocols.
type Blob []byte

// MarshalText encodes b as base64, for JSON and XML alike.
func (b Blob) MarshalText() ([]byte, error) {
	return []byte(base64.StdEncoding.EncodeToString(b)), nil
}

// UnmarshalText decodes base64.
func (b *Blob) UnmarshalText(text []byte) error {
	out, err := base64.StdEncoding.DecodeString(string(text))
	if err != nil {
		return err //nolint:wrapcheck // the decoder reports it as a serialization error
	}
	*b = out
	return nil
}

// MessageAttributeValue is a message attribute's or message system
// attribute's value. StringListValues and BinaryListValues are reserved by
// SQS and refused.
type MessageAttributeValue struct {
	StringValue      *string  `json:"StringValue,omitempty"      xml:"StringValue,omitempty"`
	DataType         string   `json:"DataType"                   xml:"DataType"`
	BinaryValue      Blob     `json:"BinaryValue,omitempty"      xml:"BinaryValue,omitempty"`
	StringListValues []string `json:"StringListValues,omitempty" xml:"-"`
	BinaryListValues []Blob   `json:"BinaryListValues,omitempty" xml:"-"`
}

// AttributeMap is a map of names to strings: queue attributes and message
// system attributes. In XML each entry is an element of the field's name
// holding Name and Value.
type AttributeMap map[string]string

// MarshalXML writes one start element per entry, in name order.
func (m AttributeMap) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	for _, k := range sortedKeys(m) {
		entry := struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		}{k, m[k]}
		if err := e.EncodeElement(entry, start); err != nil {
			return err //nolint:wrapcheck // the encoder's error, reported by Write
		}
	}
	return nil
}

// TagMap is a queue's tags. In XML each entry is a Tag holding Key and
// Value.
type TagMap map[string]string

// MarshalXML writes one start element per tag, in key order.
func (m TagMap) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	for _, k := range sortedKeys(m) {
		entry := struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		}{k, m[k]}
		if err := e.EncodeElement(entry, start); err != nil {
			return err //nolint:wrapcheck // the encoder's error, reported by Write
		}
	}
	return nil
}

// MessageAttributeMap is a message's attributes. In XML each entry is an
// element of the field's name holding Name and Value.
type MessageAttributeMap map[string]MessageAttributeValue

// MarshalXML writes one start element per attribute, in name order.
func (m MessageAttributeMap) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	for _, k := range sortedKeys(m) {
		entry := struct {
			Name  string                `xml:"Name"`
			Value MessageAttributeValue `xml:"Value"`
		}{k, m[k]}
		if err := e.EncodeElement(entry, start); err != nil {
			return err //nolint:wrapcheck // the encoder's error, reported by Write
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// CreateQueueRequest is the input of CreateQueue. The model names the tags
// member "tags", in lower case.
type CreateQueueRequest struct {
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"tags"`
	QueueName  *string           `json:"QueueName"`
}

// QueueURLResult is the result of CreateQueue and GetQueueUrl.
type QueueURLResult struct {
	QueueURL string `json:"QueueUrl" xml:"QueueUrl"`
}

// GetQueueURLRequest is the input of GetQueueUrl.
type GetQueueURLRequest struct {
	QueueName              *string `json:"QueueName"`
	QueueOwnerAWSAccountID *string `json:"QueueOwnerAWSAccountId"`
}

// QueueRequest is the input of the operations that take nothing but a
// queue: DeleteQueue, PurgeQueue and ListQueueTags.
type QueueRequest struct {
	QueueURL *string `json:"QueueUrl"`
}

// ListQueuesRequest is the input of ListQueues.
type ListQueuesRequest struct {
	MaxResults      *int    `json:"MaxResults"`
	QueueNamePrefix *string `json:"QueueNamePrefix"`
	NextToken       *string `json:"NextToken"`
}

// ListQueuesResult is the result of ListQueues.
type ListQueuesResult struct {
	NextToken string   `json:"NextToken,omitempty" xml:"NextToken,omitempty"`
	QueueURLs []string `json:"QueueUrls,omitempty" xml:"QueueUrl"`
}

// ListDeadLetterSourceQueuesRequest is the input of
// ListDeadLetterSourceQueues.
type ListDeadLetterSourceQueuesRequest struct {
	MaxResults *int    `json:"MaxResults"`
	QueueURL   *string `json:"QueueUrl"`
	NextToken  *string `json:"NextToken"`
}

// ListDeadLetterSourceQueuesResult is the result of
// ListDeadLetterSourceQueues. The model names the list "queueUrls", in
// lower case, and requires it.
type ListDeadLetterSourceQueuesResult struct {
	NextToken string   `json:"NextToken,omitempty" xml:"NextToken,omitempty"`
	QueueURLs []string `json:"queueUrls"           xml:"QueueUrl"`
}

// GetQueueAttributesRequest is the input of GetQueueAttributes.
type GetQueueAttributesRequest struct {
	QueueURL       *string  `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

// GetQueueAttributesResult is the result of GetQueueAttributes.
type GetQueueAttributesResult struct {
	Attributes AttributeMap `json:"Attributes,omitempty" xml:"Attribute,omitempty"`
}

// SetQueueAttributesRequest is the input of SetQueueAttributes.
type SetQueueAttributesRequest struct {
	Attributes map[string]string `json:"Attributes"`
	QueueURL   *string           `json:"QueueUrl"`
}

// TagQueueRequest is the input of TagQueue.
type TagQueueRequest struct {
	Tags     map[string]string `json:"Tags"`
	QueueURL *string           `json:"QueueUrl"`
}

// UntagQueueRequest is the input of UntagQueue.
type UntagQueueRequest struct {
	QueueURL *string  `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

// ListQueueTagsResult is the result of ListQueueTags.
type ListQueueTagsResult struct {
	Tags TagMap `json:"Tags,omitempty" xml:"Tag,omitempty"`
}

// AddPermissionRequest is the input of AddPermission.
type AddPermissionRequest struct {
	QueueURL      *string  `json:"QueueUrl"`
	Label         *string  `json:"Label"`
	AWSAccountIDs []string `json:"AWSAccountIds"`
	Actions       []string `json:"Actions"`
}

// RemovePermissionRequest is the input of RemovePermission.
type RemovePermissionRequest struct {
	QueueURL *string `json:"QueueUrl"`
	Label    *string `json:"Label"`
}

// SendMessageRequest is the input of SendMessage.
type SendMessageRequest struct {
	MessageAttributes       map[string]MessageAttributeValue `json:"MessageAttributes"`
	MessageSystemAttributes map[string]MessageAttributeValue `json:"MessageSystemAttributes"`
	DelaySeconds            *int                             `json:"DelaySeconds"`
	QueueURL                *string                          `json:"QueueUrl"`
	MessageBody             *string                          `json:"MessageBody"`
	MessageDeduplicationID  *string                          `json:"MessageDeduplicationId"`
	MessageGroupID          *string                          `json:"MessageGroupId"`
}

// SendMessageResult is the result of SendMessage.
type SendMessageResult struct {
	MessageID                    string `json:"MessageId"                              xml:"MessageId"`
	MD5OfMessageBody             string `json:"MD5OfMessageBody"                       xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes       string `json:"MD5OfMessageAttributes,omitempty"       xml:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes,omitempty" xml:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               string `json:"SequenceNumber,omitempty"               xml:"SequenceNumber,omitempty"`
}

// SendMessageBatchRequestEntry is one entry of SendMessageBatch.
type SendMessageBatchRequestEntry struct {
	MessageAttributes       map[string]MessageAttributeValue `json:"MessageAttributes"`
	MessageSystemAttributes map[string]MessageAttributeValue `json:"MessageSystemAttributes"`
	DelaySeconds            *int                             `json:"DelaySeconds"`
	ID                      *string                          `json:"Id"`
	MessageBody             *string                          `json:"MessageBody"`
	MessageDeduplicationID  *string                          `json:"MessageDeduplicationId"`
	MessageGroupID          *string                          `json:"MessageGroupId"`
}

// SendMessageBatchRequest is the input of SendMessageBatch.
type SendMessageBatchRequest struct {
	QueueURL *string                        `json:"QueueUrl"`
	Entries  []SendMessageBatchRequestEntry `json:"Entries"`
}

// SendMessageBatchResultEntry is one sent entry of SendMessageBatch.
type SendMessageBatchResultEntry struct {
	ID string `json:"Id" xml:"Id"`
	SendMessageResult
}

// BatchResultErrorEntry is one failed entry of a batch operation.
type BatchResultErrorEntry struct {
	ID          string `json:"Id"                xml:"Id"`
	Code        string `json:"Code"              xml:"Code"`
	Message     string `json:"Message,omitempty" xml:"Message,omitempty"`
	SenderFault bool   `json:"SenderFault"       xml:"SenderFault"`
}

// SendMessageBatchResult is the result of SendMessageBatch. Both lists are
// required, so they are sent even when empty.
type SendMessageBatchResult struct {
	Successful []SendMessageBatchResultEntry `json:"Successful" xml:"SendMessageBatchResultEntry"`
	Failed     []BatchResultErrorEntry       `json:"Failed"     xml:"BatchResultErrorEntry"`
}

// ReceiveMessageRequest is the input of ReceiveMessage. AttributeNames is
// the deprecated member MessageSystemAttributeNames replaces; both are read.
type ReceiveMessageRequest struct {
	MaxNumberOfMessages         *int     `json:"MaxNumberOfMessages"`
	VisibilityTimeout           *int     `json:"VisibilityTimeout"`
	WaitTimeSeconds             *int     `json:"WaitTimeSeconds"`
	QueueURL                    *string  `json:"QueueUrl"`
	ReceiveRequestAttemptID     *string  `json:"ReceiveRequestAttemptId"`
	AttributeNames              []string `json:"AttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	MessageAttributeNames       []string `json:"MessageAttributeNames"`
}

// Message is a received message.
type Message struct {
	Attributes             AttributeMap        `json:"Attributes,omitempty"             xml:"Attribute,omitempty"`
	MessageAttributes      MessageAttributeMap `json:"MessageAttributes,omitempty"      xml:"MessageAttribute,omitempty"`
	MessageID              string              `json:"MessageId"                        xml:"MessageId"`
	ReceiptHandle          string              `json:"ReceiptHandle"                    xml:"ReceiptHandle"`
	MD5OfBody              string              `json:"MD5OfBody"                        xml:"MD5OfBody"`
	Body                   string              `json:"Body"                             xml:"Body"`
	MD5OfMessageAttributes string              `json:"MD5OfMessageAttributes,omitempty" xml:"MD5OfMessageAttributes,omitempty"`
}

// ReceiveMessageResult is the result of ReceiveMessage.
type ReceiveMessageResult struct {
	Messages []Message `json:"Messages,omitempty" xml:"Message"`
}

// DeleteMessageRequest is the input of DeleteMessage.
type DeleteMessageRequest struct {
	QueueURL      *string `json:"QueueUrl"`
	ReceiptHandle *string `json:"ReceiptHandle"`
}

// DeleteMessageBatchRequestEntry is one entry of DeleteMessageBatch.
type DeleteMessageBatchRequestEntry struct {
	ID            *string `json:"Id"`
	ReceiptHandle *string `json:"ReceiptHandle"`
}

// DeleteMessageBatchRequest is the input of DeleteMessageBatch.
type DeleteMessageBatchRequest struct {
	QueueURL *string                          `json:"QueueUrl"`
	Entries  []DeleteMessageBatchRequestEntry `json:"Entries"`
}

// BatchResultEntry is one successful entry of DeleteMessageBatch or
// ChangeMessageVisibilityBatch.
type BatchResultEntry struct {
	ID string `json:"Id" xml:"Id"`
}

// DeleteMessageBatchResult is the result of DeleteMessageBatch.
type DeleteMessageBatchResult struct {
	Successful []BatchResultEntry      `json:"Successful" xml:"DeleteMessageBatchResultEntry"`
	Failed     []BatchResultErrorEntry `json:"Failed"     xml:"BatchResultErrorEntry"`
}

// ChangeMessageVisibilityRequest is the input of ChangeMessageVisibility.
type ChangeMessageVisibilityRequest struct {
	VisibilityTimeout *int    `json:"VisibilityTimeout"`
	QueueURL          *string `json:"QueueUrl"`
	ReceiptHandle     *string `json:"ReceiptHandle"`
}

// ChangeMessageVisibilityBatchRequestEntry is one entry of
// ChangeMessageVisibilityBatch.
type ChangeMessageVisibilityBatchRequestEntry struct {
	VisibilityTimeout *int    `json:"VisibilityTimeout"`
	ID                *string `json:"Id"`
	ReceiptHandle     *string `json:"ReceiptHandle"`
}

// ChangeMessageVisibilityBatchRequest is the input of
// ChangeMessageVisibilityBatch.
type ChangeMessageVisibilityBatchRequest struct {
	QueueURL *string                                    `json:"QueueUrl"`
	Entries  []ChangeMessageVisibilityBatchRequestEntry `json:"Entries"`
}

// ChangeMessageVisibilityBatchResult is the result of
// ChangeMessageVisibilityBatch.
type ChangeMessageVisibilityBatchResult struct {
	Successful []BatchResultEntry      `json:"Successful" xml:"ChangeMessageVisibilityBatchResultEntry"`
	Failed     []BatchResultErrorEntry `json:"Failed"     xml:"BatchResultErrorEntry"`
}
