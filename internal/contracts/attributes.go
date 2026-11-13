package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// The queue attributes SQS defines (the QueueAttributeName enum of the SQS
// model).
const (
	AttrAll                        = "All"
	AttrPolicy                     = "Policy"
	AttrVisibilityTimeout          = "VisibilityTimeout"
	AttrMaximumMessageSize         = "MaximumMessageSize"
	AttrMessageRetentionPeriod     = "MessageRetentionPeriod"
	AttrApproximateMessages        = "ApproximateNumberOfMessages"
	AttrApproximateNotVisible      = "ApproximateNumberOfMessagesNotVisible"
	AttrCreatedTimestamp           = "CreatedTimestamp"
	AttrLastModifiedTimestamp      = "LastModifiedTimestamp"
	AttrQueueArn                   = "QueueArn"
	AttrApproximateDelayed         = "ApproximateNumberOfMessagesDelayed"
	AttrDelaySeconds               = "DelaySeconds"
	AttrReceiveMessageWaitTime     = "ReceiveMessageWaitTimeSeconds"
	AttrRedrivePolicy              = "RedrivePolicy"
	AttrFifoQueue                  = "FifoQueue"
	AttrContentBasedDeduplication  = "ContentBasedDeduplication"
	AttrKmsMasterKeyID             = "KmsMasterKeyId"
	AttrKmsDataKeyReusePeriod      = "KmsDataKeyReusePeriodSeconds"
	AttrDeduplicationScope         = "DeduplicationScope"
	AttrFifoThroughputLimit        = "FifoThroughputLimit"
	AttrRedriveAllowPolicy         = "RedriveAllowPolicy"
	AttrSqsManagedSseEnabled       = "SqsManagedSseEnabled"
	DeduplicationScopeQueue        = "queue"
	DeduplicationScopeMessageGroup = "messageGroup"
	ThroughputPerQueue             = "perQueue"
	ThroughputPerMessageGroupID    = "perMessageGroupId"
	RedrivePermissionAllowAll      = "allowAll"
	RedrivePermissionDenyAll       = "denyAll"
	RedrivePermissionByQueue       = "byQueue"
)

// Defaults returns the attributes a new queue has unless the request sets
// them, as the SQS API reference gives the defaults. SqsManagedSseEnabled
// is true, as SQS sets it on a new queue.
func Defaults(fifo bool) map[string]string {
	attrs := map[string]string{
		AttrDelaySeconds:           "0",
		AttrMaximumMessageSize:     "1048576",
		AttrMessageRetentionPeriod: "345600",
		AttrReceiveMessageWaitTime: "0",
		AttrVisibilityTimeout:      "30",
		AttrSqsManagedSseEnabled:   "true",
	}
	if fifo {
		attrs[AttrFifoQueue] = "true"
		attrs[AttrContentBasedDeduplication] = "false"
		attrs[AttrDeduplicationScope] = DeduplicationScopeQueue
		attrs[AttrFifoThroughputLimit] = ThroughputPerQueue
	}
	return attrs
}

// RedrivePolicy is a queue's RedrivePolicy attribute.
type RedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

// RedriveAllowPolicy is a queue's RedriveAllowPolicy attribute.
type RedriveAllowPolicy struct {
	RedrivePermission string   `json:"redrivePermission"`
	SourceQueueArns   []string `json:"sourceQueueArns,omitempty"`
}

// Settings are the attributes that decide how a queue behaves, read from
// its stored attributes.
type Settings struct {
	Redrive                   *RedrivePolicy
	AllowPolicy               *RedriveAllowPolicy
	DeduplicationScope        string
	Delay                     time.Duration
	Retention                 time.Duration
	Wait                      time.Duration
	VisibilityTimeout         time.Duration
	MaximumMessageSize        int
	ContentBasedDeduplication bool
}

// SettingsOf reads the settings from a queue's attributes, which the store
// keeps normalized, defaults included. A value that does not parse means
// the store is corrupt, and is an error.
func SettingsOf(attrs map[string]string) (Settings, error) {
	var s Settings
	durations := map[string]*time.Duration{
		AttrDelaySeconds:           &s.Delay,
		AttrMessageRetentionPeriod: &s.Retention,
		AttrReceiveMessageWaitTime: &s.Wait,
		AttrVisibilityTimeout:      &s.VisibilityTimeout,
	}
	for name, dst := range durations {
		n, err := strconv.Atoi(attrs[name])
		if err != nil {
			return s, fmt.Errorf("queues: stored attribute %s: %w", name, err)
		}
		*dst = time.Duration(n) * time.Second
	}
	var err error
	if s.MaximumMessageSize, err = strconv.Atoi(attrs[AttrMaximumMessageSize]); err != nil {
		return s, fmt.Errorf("queues: stored attribute %s: %w", AttrMaximumMessageSize, err)
	}
	s.ContentBasedDeduplication = attrs[AttrContentBasedDeduplication] == "true"
	s.DeduplicationScope = attrs[AttrDeduplicationScope]
	if s.Redrive, err = decodePolicy[RedrivePolicy](attrs, AttrRedrivePolicy); err != nil {
		return s, err
	}
	s.AllowPolicy, err = decodePolicy[RedriveAllowPolicy](attrs, AttrRedriveAllowPolicy)
	return s, err
}

// decodePolicy decodes the JSON attribute name, or returns nil when the
// queue does not have it.
func decodePolicy[T any](attrs map[string]string, name string) (*T, error) {
	v := attrs[name]
	if v == "" {
		return nil, nil
	}
	var p T
	if err := json.Unmarshal([]byte(v), &p); err != nil {
		return nil, fmt.Errorf("queues: stored attribute %s: %w", name, err)
	}
	return &p, nil
}
