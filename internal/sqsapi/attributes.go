package sqsapi

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"

	"tule.dev/queues/internal/contracts"
)

// intRange is the range an integer queue attribute takes, from the
// CreateQueue and SetQueueAttributes API reference.
type intRange struct{ lo, hi int }

var intAttributes = map[string]intRange{
	contracts.AttrDelaySeconds:           {0, MaxDelaySeconds},
	contracts.AttrMaximumMessageSize:     {1024, MaxMessageSize},
	contracts.AttrMessageRetentionPeriod: {60, 1209600},
	contracts.AttrReceiveMessageWaitTime: {0, MaxWaitTimeSeconds},
	contracts.AttrVisibilityTimeout:      {0, MaxVisibilityTimeout},
	contracts.AttrKmsDataKeyReusePeriod:  {60, 86400},
}

// fifoAttributes are the attributes only a FIFO queue has.
var fifoAttributes = []string{
	contracts.AttrContentBasedDeduplication,
	contracts.AttrDeduplicationScope,
	contracts.AttrFifoThroughputLimit,
}

// settable reports whether a queue of the type takes name in CreateQueue
// or SetQueueAttributes.
func settable(name string, fifo bool) bool {
	if _, ok := intAttributes[name]; ok {
		return true
	}
	switch name {
	case contracts.AttrPolicy, contracts.AttrRedrivePolicy, contracts.AttrRedriveAllowPolicy,
		contracts.AttrKmsMasterKeyID, contracts.AttrSqsManagedSseEnabled:
		return true
	}
	return fifo && slices.Contains(fifoAttributes, name)
}

// CheckCreateAttributes checks CreateQueue's attributes for a queue of the
// name, and returns them normalized and whether the queue is FIFO. A queue
// is FIFO when FifoQueue is true, which only a name with the .fifo suffix
// may be, and a .fifo name must be.
func CheckCreateAttributes(name string, attrs map[string]string) (map[string]string, bool, error) {
	nameFIFO, err := CheckQueueName(name)
	if err != nil {
		return nil, false, err
	}
	fifo := false
	if v, ok := attrs[contracts.AttrFifoQueue]; ok {
		b, bErr := checkBool(contracts.AttrFifoQueue, v)
		if bErr != nil {
			return nil, false, bErr
		}
		fifo = b == "true"
	}
	if fifo != nameFIFO {
		return nil, false, ErrInvalidParameterValue("The name of a FIFO queue can only " +
			"include alphanumeric characters, hyphens, or underscores, must end with .fifo " +
			"suffix and be 1 to 80 in length. A FIFO queue must set FifoQueue to true.")
	}
	rest := maps.Clone(attrs)
	delete(rest, contracts.AttrFifoQueue)
	out, err := CheckQueueAttributes(rest, contracts.Defaults(fifo), fifo)
	if err != nil {
		return nil, false, err
	}
	if fifo {
		out[contracts.AttrFifoQueue] = "true"
	}
	return out, fifo, nil
}

// CheckQueueAttributes checks attributes to set on a queue of the type
// whose attributes are current, and returns them normalized. A value of ""
// for an optional attribute (Policy, RedrivePolicy, RedriveAllowPolicy,
// KmsMasterKeyId) removes it.
//
//nolint:gocyclo,cyclop // one case per attribute kind
func CheckQueueAttributes(attrs, current map[string]string, fifo bool) (map[string]string, error) {
	out := make(map[string]string, len(attrs))
	for _, name := range sortedKeys(attrs) {
		v := attrs[name]
		if !settable(name, fifo) {
			return nil, ErrInvalidAttributeName(name)
		}
		var err error
		switch name {
		case contracts.AttrSqsManagedSseEnabled, contracts.AttrContentBasedDeduplication:
			v, err = checkBool(name, v)
		case contracts.AttrDeduplicationScope:
			err = checkEnum(name, v, contracts.DeduplicationScopeMessageGroup,
				contracts.DeduplicationScopeQueue)
		case contracts.AttrFifoThroughputLimit:
			err = checkEnum(name, v, contracts.ThroughputPerQueue,
				contracts.ThroughputPerMessageGroupID)
		case contracts.AttrRedrivePolicy:
			v, err = checkRedrivePolicy(v)
		case contracts.AttrRedriveAllowPolicy:
			v, err = checkRedriveAllowPolicy(v)
		case contracts.AttrPolicy, contracts.AttrKmsMasterKeyID:
		default:
			v, err = checkInt(name, v, intAttributes[name])
		}
		if err != nil {
			return nil, err
		}
		out[name] = v
	}

	merged := maps.Clone(current)
	maps.Copy(merged, out)
	if merged[contracts.AttrFifoThroughputLimit] == contracts.ThroughputPerMessageGroupID &&
		merged[contracts.AttrDeduplicationScope] != contracts.DeduplicationScopeMessageGroup {
		return nil, ErrInvalidAttributeValue(contracts.AttrFifoThroughputLimit,
			"The perMessageGroupId value is allowed only when the value for "+
				"DeduplicationScope is messageGroup.")
	}
	return out, nil
}

func checkInt(name, v string, r intRange) (string, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n < r.lo || n > r.hi {
		return "", ErrInvalidAttributeValue(name, "")
	}
	return strconv.Itoa(n), nil
}

func checkBool(name, v string) (string, error) {
	switch strings.ToLower(v) {
	case "true":
		return "true", nil
	case "false":
		return "false", nil
	}
	return "", ErrInvalidAttributeValue(name, "")
}

func checkEnum(name, v string, values ...string) error {
	if !slices.Contains(values, v) {
		return ErrInvalidAttributeValue(name, "")
	}
	return nil
}

// decodeObject decodes a JSON policy attribute into an object, refusing
// any member outside known.
func decodeObject(name, v string, known ...string) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader([]byte(v)))
	if err := dec.Decode(&obj); err != nil || obj == nil || dec.More() {
		return nil, errInvalidValue(name, v, name+" must be a JSON object")
	}
	for k := range obj {
		if !slices.Contains(known, k) {
			return nil, errInvalidValue(name, v, "Only following attributes are supported: ["+
				strings.Join(known, ", ")+"]")
		}
	}
	return obj, nil
}

// checkRedrivePolicy checks a RedrivePolicy: deadLetterTargetArn, and
// maxReceiveCount from 1 to 1000, which may be sent as a number or a
// string and defaults to 10. It returns it normalized, with
// maxReceiveCount a number. Whether the target exists is the store's to
// check.
func checkRedrivePolicy(v string) (string, error) {
	if v == "" {
		return "", nil
	}
	obj, err := decodeObject(contracts.AttrRedrivePolicy, v, "deadLetterTargetArn",
		"maxReceiveCount")
	if err != nil {
		return "", err
	}
	p := contracts.RedrivePolicy{MaxReceiveCount: defaultMaxReceiveCount}
	raw, ok := obj["deadLetterTargetArn"]
	if !ok || json.Unmarshal(raw, &p.DeadLetterTargetArn) != nil || p.DeadLetterTargetArn == "" {
		return "", errInvalidValue(contracts.AttrRedrivePolicy, v,
			"Redrive policy is missing a deadLetterTargetArn")
	}
	if raw, ok = obj["maxReceiveCount"]; ok {
		n, nErr := jsonInt(raw)
		if nErr != nil || n < 1 || n > maxReceiveCountLimit {
			return "", errInvalidValue(contracts.AttrRedrivePolicy, v,
				"Invalid value for maxReceiveCount: must be an integer from 1 to 1000")
		}
		p.MaxReceiveCount = n
	}
	out, err := json.Marshal(p)
	return string(out), err //nolint:wrapcheck // marshaling a plain struct does not fail
}

// jsonInt reads a JSON number or a string holding one.
func jsonInt(raw json.RawMessage) (int, error) {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return strconv.Atoi(s) //nolint:wrapcheck // the caller reports it as the value's error
	}
	var n int
	err := json.Unmarshal(raw, &n)
	return n, err //nolint:wrapcheck // the caller reports it as the value's error
}

// checkRedriveAllowPolicy checks a RedriveAllowPolicy: redrivePermission
// allowAll, denyAll or byQueue, and with byQueue only, 1 to 10
// sourceQueueArns.
func checkRedriveAllowPolicy(v string) (string, error) {
	if v == "" {
		return "", nil
	}
	if _, err := decodeObject(contracts.AttrRedriveAllowPolicy, v, "redrivePermission",
		"sourceQueueArns"); err != nil {
		return "", err
	}
	invalid := func(reason string) (string, error) {
		return "", errInvalidValue(contracts.AttrRedriveAllowPolicy, v, reason)
	}
	var p contracts.RedriveAllowPolicy
	if json.Unmarshal([]byte(v), &p) != nil {
		return invalid("redrivePermission must be a string, and sourceQueueArns a list of " +
			"strings")
	}
	if reason := allowPolicyFault(&p); reason != "" {
		return invalid(reason)
	}
	out, err := json.Marshal(p)
	return string(out), err //nolint:wrapcheck // marshaling a plain struct does not fail
}

// allowPolicyFault returns what is wrong with a RedriveAllowPolicy, or ""
// when nothing is.
func allowPolicyFault(p *contracts.RedriveAllowPolicy) string {
	switch p.RedrivePermission {
	case contracts.RedrivePermissionByQueue:
		if n := len(p.SourceQueueArns); n == 0 || n > maxRedriveSources {
			return "byQueue requires 1 to 10 sourceQueueArns"
		}
	case contracts.RedrivePermissionAllowAll, contracts.RedrivePermissionDenyAll:
		if p.SourceQueueArns != nil {
			return "sourceQueueArns can be specified only when redrivePermission is byQueue"
		}
	default:
		return "redrivePermission must be allowAll, denyAll or byQueue"
	}
	return ""
}

// queueAttributeNames are the names GetQueueAttributes takes, in the order
// of the SQS model's QueueAttributeName enum.
var queueAttributeNames = []string{
	contracts.AttrPolicy, contracts.AttrVisibilityTimeout, contracts.AttrMaximumMessageSize,
	contracts.AttrMessageRetentionPeriod, contracts.AttrApproximateMessages,
	contracts.AttrApproximateNotVisible, contracts.AttrCreatedTimestamp,
	contracts.AttrLastModifiedTimestamp, contracts.AttrQueueArn,
	contracts.AttrApproximateDelayed, contracts.AttrDelaySeconds,
	contracts.AttrReceiveMessageWaitTime, contracts.AttrRedrivePolicy, contracts.AttrFifoQueue,
	contracts.AttrContentBasedDeduplication, contracts.AttrKmsMasterKeyID,
	contracts.AttrKmsDataKeyReusePeriod, contracts.AttrDeduplicationScope,
	contracts.AttrFifoThroughputLimit, contracts.AttrRedriveAllowPolicy,
	contracts.AttrSqsManagedSseEnabled,
}

// AllAttributes returns every attribute the queue has, as GetQueueAttributes
// reports them for All: the stored ones, the counts, the timestamps in epoch
// seconds and the ARN.
func AllAttributes(q *contracts.Queue, arn string) AttributeMap {
	out := AttributeMap{}
	maps.Copy(out, q.Attributes)
	out[contracts.AttrApproximateMessages] = strconv.Itoa(q.Counts.Visible)
	out[contracts.AttrApproximateNotVisible] = strconv.Itoa(q.Counts.InFlight)
	out[contracts.AttrApproximateDelayed] = strconv.Itoa(q.Counts.Delayed)
	out[contracts.AttrCreatedTimestamp] = strconv.FormatInt(q.CreatedAt.Unix(), 10)
	out[contracts.AttrLastModifiedTimestamp] = strconv.FormatInt(q.ModifiedAt.Unix(), 10)
	out[contracts.AttrQueueArn] = arn
	return out
}

// QueueAttributes returns the attributes names asks for: All, or a list of
// names of the SQS enum. An attribute the queue does not have, such as an
// unset RedrivePolicy or a FIFO attribute of a standard queue, is left out.
// No names asks for nothing, as SQS answers.
func QueueAttributes(q *contracts.Queue, arn string, names []string) (AttributeMap, error) {
	for _, n := range names {
		if n != contracts.AttrAll && !slices.Contains(queueAttributeNames, n) {
			return nil, ErrInvalidAttributeName(n)
		}
	}
	all := AllAttributes(q, arn)
	if slices.Contains(names, contracts.AttrAll) {
		return all, nil
	}
	out := AttributeMap{}
	for _, n := range names {
		if v, ok := all[n]; ok {
			out[n] = v
		}
	}
	return out, nil
}
