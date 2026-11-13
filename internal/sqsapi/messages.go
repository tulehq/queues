package sqsapi

import (
	"slices"
	"strconv"
	"strings"

	"tule.dev/queues/internal/contracts"
)

// The message system attributes ReceiveMessage returns (the
// MessageSystemAttributeName enum of the SQS model).
const (
	SysAll                    = "All"
	SysSenderID               = "SenderId"
	SysSentTimestamp          = "SentTimestamp"
	SysApproximateReceive     = "ApproximateReceiveCount"
	SysApproximateFirst       = "ApproximateFirstReceiveTimestamp"
	SysSequenceNumber         = "SequenceNumber"
	SysMessageDeduplicationID = "MessageDeduplicationId"
	SysMessageGroupID         = "MessageGroupId"
	SysAWSTraceHeader         = "AWSTraceHeader"
	SysDeadLetterSourceArn    = "DeadLetterQueueSourceArn"
)

var systemAttributeNames = []string{
	SysAll, SysSenderID, SysSentTimestamp, SysApproximateReceive, SysApproximateFirst,
	SysSequenceNumber, SysMessageDeduplicationID, SysMessageGroupID, SysAWSTraceHeader,
	SysDeadLetterSourceArn,
}

// CheckSystemAttributeNames checks the system attribute names a
// ReceiveMessage asks for.
func CheckSystemAttributeNames(names []string) error {
	for _, n := range names {
		if !slices.Contains(systemAttributeNames, n) {
			return ErrInvalidAttributeName(n)
		}
	}
	return nil
}

// SystemAttributes returns every system attribute a message has, epoch
// milliseconds for the timestamps, as ReceiveMessage reports them for All.
func SystemAttributes(m *contracts.Message) map[string]string {
	out := map[string]string{
		SysSenderID:           m.SenderID,
		SysSentTimestamp:      strconv.FormatInt(m.SentAt.UnixMilli(), 10),
		SysApproximateReceive: strconv.Itoa(m.ReceiveCount),
	}
	if !m.FirstReceivedAt.IsZero() {
		out[SysApproximateFirst] = strconv.FormatInt(m.FirstReceivedAt.UnixMilli(), 10)
	}
	set := func(name, v string) {
		if v != "" {
			out[name] = v
		}
	}
	set(SysSequenceNumber, m.SequenceNumber)
	set(SysMessageDeduplicationID, m.DeduplicationID)
	set(SysMessageGroupID, m.GroupID)
	set(SysDeadLetterSourceArn, m.DeadLetterSourceARN)
	if a, ok := m.SystemAttributes[SysAWSTraceHeader]; ok && a.StringValue != nil {
		out[SysAWSTraceHeader] = *a.StringValue
	}
	return out
}

// wantsAttribute reports whether a message attribute name matches one of
// the names a ReceiveMessage asks for: All, .*, a prefix ending in .*, or
// the name itself.
func wantsAttribute(names []string, name string) bool {
	for _, n := range names {
		switch {
		case n == "All" || n == ".*" || n == name:
			return true
		case strings.HasSuffix(n, ".*") && strings.HasPrefix(name, strings.TrimSuffix(n, "*")):
			return true
		}
	}
	return false
}

// Received returns a received message as ReceiveMessage reports it, with
// the system attributes and the message attributes the request asks for.
// MD5OfMessageAttributes is the digest of the attributes returned, so it
// matches what the client receives.
func Received(m *contracts.Message, systemNames, attributeNames []string) Message {
	out := Message{
		MessageID:     m.ID,
		ReceiptHandle: m.ReceiptHandle,
		MD5OfBody:     m.MD5OfBody,
		Body:          m.Body,
	}
	if len(systemNames) > 0 {
		all := SystemAttributes(m)
		out.Attributes = AttributeMap{}
		for k, v := range all {
			if slices.Contains(systemNames, SysAll) || slices.Contains(systemNames, k) {
				out.Attributes[k] = v
			}
		}
	}
	picked := map[string]contracts.MessageAttribute{}
	for name, a := range m.Attributes {
		if wantsAttribute(attributeNames, name) {
			picked[name] = a
		}
	}
	if len(picked) > 0 {
		out.MessageAttributes = MessageAttributeMap{}
		for name, a := range picked {
			out.MessageAttributes[name] = WireAttribute(a)
		}
		out.MD5OfMessageAttributes = MD5OfAttributes(picked)
	}
	return out
}

// WireAttribute returns a stored attribute as the wire carries it.
func WireAttribute(a contracts.MessageAttribute) MessageAttributeValue {
	return MessageAttributeValue{
		DataType: a.DataType, StringValue: a.StringValue, BinaryValue: a.BinaryValue,
	}
}

// actionNames are the actions AddPermission grants, the SQS actions a queue
// policy can name.
var actionNames = []string{
	"*", "SendMessage", "ReceiveMessage", "DeleteMessage", "ChangeMessageVisibility",
	"GetQueueAttributes", "GetQueueUrl", "ListDeadLetterSourceQueues", "PurgeQueue",
	"ListQueueTags", "SetQueueAttributes", "TagQueue", "UntagQueue", "DeleteQueue",
	"AddPermission", "RemovePermission", "CreateQueue", "ListQueues",
}

// CheckPermission checks an AddPermission: a label of up to 80 letters,
// digits, hyphens and underscores, account IDs of 12 digits, and up to
// seven actions SQS has.
func CheckPermission(req *AddPermissionRequest) (contracts.Permission, error) {
	var p contracts.Permission
	label, err := Required("Label", req.Label)
	if err != nil {
		return p, err
	}
	if err = CheckLabel(label); err != nil {
		return p, err
	}
	if len(req.AWSAccountIDs) == 0 {
		return p, ErrMissingParameter("AWSAccountIds")
	}
	if len(req.Actions) == 0 {
		return p, ErrMissingParameter("Actions")
	}
	for _, id := range req.AWSAccountIDs {
		if !isAccountID(id) {
			return p, errInvalidValue("AWSAccountIds", id, "Must be a 12-digit AWS account ID")
		}
	}
	if len(req.Actions) > maxPermissionActions {
		return p, ErrOverLimit("An Amazon SQS policy can have a maximum of 7 actions per " +
			"statement.")
	}
	for _, a := range req.Actions {
		if !slices.Contains(actionNames, a) {
			return p, errInvalidValue("Actions", a, "Not a valid SQS action")
		}
	}
	return contracts.Permission{
		Label: label, AccountIDs: req.AWSAccountIDs,
		Actions: req.Actions,
	}, nil
}

// CheckLabel checks a permission label.
func CheckLabel(label string) error {
	if !labelPattern.MatchString(label) {
		return errInvalidValue("Label", label, "Can only include alphanumeric characters, "+
			"hyphens, or underscores. 1 to 80 in length")
	}
	return nil
}

func isAccountID(s string) bool {
	if len(s) != 12 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
