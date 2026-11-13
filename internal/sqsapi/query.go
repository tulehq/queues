package sqsapi

import (
	"encoding/base64"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Params are the parameters of a Query request: its form fields.
//
// The Query names of the structured members come from the SQS model's
// xmlName and xmlFlattened traits, as the awsQuery protocol serializes them:
// a flattened list is Name.N, a flattened map is Name.N.Name and
// Name.N.Value (Name.N.Key and Name.N.Value for tags), and a list of
// structures is Name.N.Member, N counting from 1. The API reference's
// examples also show the lists and maps of one item without the index
// (AttributeName=All, Attribute.Name=..., Tag.Key=...), which are read too.
type Params url.Values

// canonicalIndex is the one spelling of a list index the form accepts.
var canonicalIndex = regexp.MustCompile(`^[1-9][0-9]*$`)

// CheckForm refuses a Query form that could be read two ways, with
// MalformedQueryString, before anything reads it:
//
//   - a list index spelled other than [1-9][0-9]* (Tag.01, Tag.+1, Tag.0),
//     which strconv.Atoi would read as the index of another spelling, the
//     later in map order winning;
//   - a list given both indexed and in the one-item shorthand (Tag.1.Key
//     and Tag.Key, or AttributeName.1 and AttributeName), which would be
//     read as two items or one.
//
// A segment is an index when strconv.Atoi reads it, as the decoders read
// them. The SQS API reference documents no answer for these forms.
//
// A parameter given more than once is not refused: its first value is
// read, every time, and authorization and the operation share one decoding.
// Some Query clients send ReceiveMessage's AttributeNames and
// MessageSystemAttributeNames both as AttributeName.N.
func CheckForm(form url.Values) error {
	seen := map[string]*formKinds{}
	for key := range form {
		if err := markKey(seen, key); err != nil {
			return err
		}
	}
	for _, k := range seen {
		if k.indexed && (k.named || k.whole) {
			return ErrMalformedQueryString()
		}
	}
	return nil
}

// formKinds records how a form's names continue a prefix: with an index,
// with a member name, or not at all (the prefix is itself a name).
type formKinds struct{ indexed, named, whole bool }

// markKey records in seen how key continues each of its prefixes, refusing
// an index that is not canonical.
func markKey(seen map[string]*formKinds, key string) error {
	at := func(prefix string) *formKinds {
		k := seen[prefix]
		if k == nil {
			k = &formKinds{}
			seen[prefix] = k
		}
		return k
	}
	at(key).whole = true
	segs := strings.Split(key, ".")
	for i := 1; i < len(segs); i++ {
		prefix := strings.Join(segs[:i], ".")
		if _, err := strconv.Atoi(segs[i]); err != nil {
			at(prefix).named = true
			continue
		}
		if !canonicalIndex.MatchString(segs[i]) {
			return ErrMalformedQueryString()
		}
		at(prefix).indexed = true
	}
	return nil
}

// get returns a parameter, or nil when the request leaves it out.
func (p Params) get(name string) *string {
	vs, ok := p[name]
	if !ok || len(vs) == 0 {
		return nil
	}
	return &vs[0]
}

// integer returns an integer parameter, or nil when the request leaves it
// out.
func (p Params) integer(name string) (*int, error) {
	v := p.get(name)
	if v == nil {
		return nil, nil
	}
	n, err := strconv.Atoi(*v)
	if err != nil {
		return nil, errInvalidValue(name, *v, "Must be an integer")
	}
	return &n, nil
}

// grouped collects the parameters under prefix by their index: prefix.N.rest
// becomes group N's rest. With unindexed, prefix.rest is group 0's rest,
// for the one-item shorthand. It returns the groups in index order.
func (p Params) grouped(prefix string, unindexed bool) []Params {
	groups := map[int]Params{}
	for name, vs := range p {
		n, sub, ok := groupKey(name, prefix, unindexed)
		if !ok {
			continue
		}
		if groups[n] == nil {
			groups[n] = Params{}
		}
		groups[n][sub] = vs
	}
	out := make([]Params, 0, len(groups))
	for _, n := range slices.Sorted(maps.Keys(groups)) {
		out = append(out, groups[n])
	}
	return out
}

// groupKey splits a parameter name under prefix into its group's index and
// the rest, reporting false for a name of no group.
func groupKey(name, prefix string, unindexed bool) (int, string, bool) {
	rest, ok := strings.CutPrefix(name, prefix+".")
	if !ok {
		return 0, "", false
	}
	index, sub, hasSub := strings.Cut(rest, ".")
	n, err := strconv.Atoi(index)
	switch {
	case err == nil && n >= 1 && hasSub:
		return n, sub, true
	case err != nil && unindexed:
		return 0, rest, true
	}
	return 0, "", false
}

// list returns the flattened list name.N, or nil when the request sets none
// of it. The unindexed name is read as one more item.
func (p Params) list(name string) []string {
	type item struct {
		value string
		n     int
	}
	var items []item
	for key, vs := range p {
		if len(vs) == 0 {
			continue
		}
		if key == name {
			items = append(items, item{n: 0, value: vs[0]})
			continue
		}
		rest, ok := strings.CutPrefix(key, name+".")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(rest)
		if err != nil || n < 1 {
			continue
		}
		items = append(items, item{n: n, value: vs[0]})
	}
	if items == nil {
		return nil
	}
	slices.SortFunc(items, func(a, b item) int { return a.n - b.n })
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.value)
	}
	return out
}

// tagMap returns the flattened tags Tag.N.Key/Tag.N.Value, or nil when the
// request sets none, refusing a key that two entries repeat.
func (p Params) tagMap() (RequestTags, error) {
	m, err := p.stringMapOf("Tag", "Key", true)
	return RequestTags(m), err
}

// stringMap returns the flattened map prefix.N.<key>/prefix.N.Value, or
// nil when the request sets none of it.
func (p Params) stringMap(prefix, key string) (map[string]string, error) {
	return p.stringMapOf(prefix, key, false)
}

// stringMapOf is stringMap; with unique, a key two entries repeat is
// refused rather than the later entry winning.
func (p Params) stringMapOf(prefix, key string, unique bool) (map[string]string, error) {
	groups := p.grouped(prefix, true)
	if len(groups) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(groups))
	for _, g := range groups {
		k := g.get(key)
		if k == nil {
			return nil, ErrMissingParameter(prefix + ".N." + key)
		}
		v := g.get("Value")
		if v == nil {
			return nil, ErrMissingParameter(prefix + ".N.Value")
		}
		if _, dup := out[*k]; dup && unique {
			return nil, errRepeatedTagKey(*k)
		}
		out[*k] = *v
	}
	return out, nil
}

// messageAttributes returns the flattened map of message attributes under
// prefix, or nil when the request sets none.
func (p Params) messageAttributes(prefix string) (map[string]MessageAttributeValue, error) {
	groups := p.grouped(prefix, true)
	if len(groups) == 0 {
		return nil, nil
	}
	out := make(map[string]MessageAttributeValue, len(groups))
	for _, g := range groups {
		name := g.get("Name")
		if name == nil {
			return nil, ErrMissingParameter(prefix + ".N.Name")
		}
		v := MessageAttributeValue{StringValue: g.get("Value.StringValue")}
		if t := g.get("Value.DataType"); t != nil {
			v.DataType = *t
		}
		if b := g.get("Value.BinaryValue"); b != nil {
			raw, err := base64.StdEncoding.DecodeString(*b)
			if err != nil {
				return nil, ErrInvalidParameterValue(fmt.Sprintf(
					"The binary value of message attribute %s is not valid base64.", *name))
			}
			v.BinaryValue = raw
		}
		v.StringListValues = g.list("Value.StringListValue")
		for _, s := range g.list("Value.BinaryListValue") {
			v.BinaryListValues = append(v.BinaryListValues, Blob(s))
		}
		out[*name] = v
	}
	return out, nil
}

// Decode reads the parameters of a Query request into v, the request
// struct of its operation.
//
//nolint:gocyclo,cyclop,funlen // one case per request shape, each a few reads
func (p Params) Decode(v any) error {
	var err error
	switch req := v.(type) {
	case *CreateQueueRequest:
		req.QueueName = p.get("QueueName")
		if req.Attributes, err = p.stringMap("Attribute", "Name"); err != nil {
			return err
		}
		req.Tags, err = p.tagMap()
	case *GetQueueURLRequest:
		req.QueueName = p.get("QueueName")
		req.QueueOwnerAWSAccountID = p.get("QueueOwnerAWSAccountId")
	case *QueueRequest:
		req.QueueURL = p.get("QueueUrl")
	case *ListQueuesRequest:
		req.QueueNamePrefix = p.get("QueueNamePrefix")
		req.NextToken = p.get("NextToken")
		req.MaxResults, err = p.integer("MaxResults")
	case *ListDeadLetterSourceQueuesRequest:
		req.QueueURL = p.get("QueueUrl")
		req.NextToken = p.get("NextToken")
		req.MaxResults, err = p.integer("MaxResults")
	case *GetQueueAttributesRequest:
		req.QueueURL = p.get("QueueUrl")
		req.AttributeNames = p.list("AttributeName")
	case *SetQueueAttributesRequest:
		req.QueueURL = p.get("QueueUrl")
		req.Attributes, err = p.stringMap("Attribute", "Name")
	case *TagQueueRequest:
		req.QueueURL = p.get("QueueUrl")
		req.Tags, err = p.tagMap()
	case *UntagQueueRequest:
		req.QueueURL = p.get("QueueUrl")
		req.TagKeys = p.list("TagKey")
	case *AddPermissionRequest:
		req.QueueURL = p.get("QueueUrl")
		req.Label = p.get("Label")
		req.AWSAccountIDs = p.list("AWSAccountId")
		req.Actions = p.list("ActionName")
	case *RemovePermissionRequest:
		req.QueueURL = p.get("QueueUrl")
		req.Label = p.get("Label")
	case *SendMessageRequest:
		req.QueueURL = p.get("QueueUrl")
		err = p.decodeSendEntry(&req.MessageBody, &req.DelaySeconds, &req.MessageAttributes,
			&req.MessageSystemAttributes, &req.MessageDeduplicationID, &req.MessageGroupID)
	case *SendMessageBatchRequest:
		req.QueueURL = p.get("QueueUrl")
		for _, g := range p.grouped("SendMessageBatchRequestEntry", false) {
			e := SendMessageBatchRequestEntry{ID: g.get("Id")}
			if err = g.decodeSendEntry(&e.MessageBody, &e.DelaySeconds, &e.MessageAttributes,
				&e.MessageSystemAttributes, &e.MessageDeduplicationID,
				&e.MessageGroupID); err != nil {
				return err
			}
			req.Entries = append(req.Entries, e)
		}
	case *ReceiveMessageRequest:
		return p.decodeReceive(req)
	case *DeleteMessageRequest:
		req.QueueURL = p.get("QueueUrl")
		req.ReceiptHandle = p.get("ReceiptHandle")
	case *DeleteMessageBatchRequest:
		req.QueueURL = p.get("QueueUrl")
		for _, g := range p.grouped("DeleteMessageBatchRequestEntry", false) {
			req.Entries = append(req.Entries, DeleteMessageBatchRequestEntry{
				ID: g.get("Id"), ReceiptHandle: g.get("ReceiptHandle"),
			})
		}
	case *ChangeMessageVisibilityRequest:
		req.QueueURL = p.get("QueueUrl")
		req.ReceiptHandle = p.get("ReceiptHandle")
		req.VisibilityTimeout, err = p.integer("VisibilityTimeout")
	case *ChangeMessageVisibilityBatchRequest:
		req.QueueURL = p.get("QueueUrl")
		for _, g := range p.grouped("ChangeMessageVisibilityBatchRequestEntry", false) {
			e := ChangeMessageVisibilityBatchRequestEntry{
				ID: g.get("Id"), ReceiptHandle: g.get("ReceiptHandle"),
			}
			if e.VisibilityTimeout, err = g.integer("VisibilityTimeout"); err != nil {
				return err
			}
			req.Entries = append(req.Entries, e)
		}
	default:
		return fmt.Errorf("sqsapi: no Query decoder for %T", v)
	}
	return err
}

func (p Params) decodeSendEntry(
	body **string,
	delay **int,
	attrs, system *map[string]MessageAttributeValue,
	dedup, group **string,
) error {
	var err error
	*body = p.get("MessageBody")
	*dedup = p.get("MessageDeduplicationId")
	*group = p.get("MessageGroupId")
	if *delay, err = p.integer("DelaySeconds"); err != nil {
		return err
	}
	if *attrs, err = p.messageAttributes("MessageAttribute"); err != nil {
		return err
	}
	*system, err = p.messageAttributes("MessageSystemAttribute")
	return err
}

// decodeReceive reads ReceiveMessage. The model serializes both
// AttributeNames and MessageSystemAttributeNames as AttributeName.N;
// MessageSystemAttributeName.N is read too.
func (p Params) decodeReceive(req *ReceiveMessageRequest) error {
	var err error
	req.QueueURL = p.get("QueueUrl")
	req.ReceiveRequestAttemptID = p.get("ReceiveRequestAttemptId")
	req.AttributeNames = p.list("AttributeName")
	req.MessageSystemAttributeNames = p.list("MessageSystemAttributeName")
	req.MessageAttributeNames = p.list("MessageAttributeName")
	if req.MaxNumberOfMessages, err = p.integer("MaxNumberOfMessages"); err != nil {
		return err
	}
	if req.VisibilityTimeout, err = p.integer("VisibilityTimeout"); err != nil {
		return err
	}
	req.WaitTimeSeconds, err = p.integer("WaitTimeSeconds")
	return err
}
