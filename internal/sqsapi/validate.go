package sqsapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"tule.dev/queues/internal/contracts"
)

// The limits of the SQS API reference and developer guide (quotas).
const (
	MaxMessageSize         = 1 << 20
	MaxBatchEntries        = 10
	MaxMessageAttributes   = 10
	maxAttributeNameLen    = 256
	maxDataTypeLen         = 256
	MaxDelaySeconds        = 900
	MaxVisibilityTimeout   = 43200
	MaxWaitTimeSeconds     = 20
	MaxReceiveMessages     = 10
	MaxListResults         = 1000
	MaxMoveRate            = 500
	MaxMoveTasksListed     = 10
	maxQueueNameLen        = 80
	maxTagKeyLen           = 128
	maxTagValueLen         = 256
	FIFOSuffix             = ".fifo"
	typeString             = "String"
	typeNumber             = "Number"
	typeBinary             = "Binary"
	reservedTagPrefix      = "aws:"
	invalidForQueueType    = "The request include parameter that is not valid for this queue type"
	attrTraceHeader        = "AWSTraceHeader"
	numberMaxDigits        = 38
	numberMaxExponent      = 126
	numberMinExponent      = -128
	maxPermissionActions   = 7
	maxRedriveSources      = 10
	maxReceiveCountLimit   = 1000
	defaultMaxReceiveCount = 10
)

var (
	queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	batchIDPattern   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	// tokenPattern is MessageDeduplicationId's, MessageGroupId's and
	// ReceiveRequestAttemptId's: up to 128 letters, digits and ASCII
	// punctuation.
	tokenPattern         = regexp.MustCompile(`^[!-~]{1,128}$`)
	attributeNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	labelPattern         = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
	numberPattern        = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)
	tagPattern           = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)
)

// CheckQueueName checks a queue name: up to 80 letters, digits, hyphens and
// underscores, with the .fifo suffix, which counts toward the 80, for a FIFO
// queue. It reports whether the name is a FIFO queue's.
func CheckQueueName(name string) (bool, error) {
	fifo := strings.HasSuffix(name, FIFOSuffix)
	base := strings.TrimSuffix(name, FIFOSuffix)
	if len(name) > maxQueueNameLen || !queueNamePattern.MatchString(base) {
		return false, ErrInvalidParameterValue("Can only include alphanumeric characters, " +
			"hyphens, or underscores. 1 to 80 in length")
	}
	return fifo, nil
}

// messageChars are the characters SQS allows in a message: #x9, #xA, #xD,
// #x20 to #xD7FF, #xE000 to #xFFFD and #x10000 to #x10FFFF.
var messageChars = &unicode.RangeTable{
	R16: []unicode.Range16{
		{Lo: 0x9, Hi: 0xA, Stride: 1},
		{Lo: 0xD, Hi: 0xD, Stride: 1},
		{Lo: 0x20, Hi: 0xD7FF, Stride: 1},
		{Lo: 0xE000, Hi: 0xFFFD, Stride: 1},
	},
	R32: []unicode.Range32{{Lo: 0x10000, Hi: 0x10FFFF, Stride: 1}},
}

// validChars reports whether s is UTF-8 holding only messageChars.
func validChars(s string) bool {
	return utf8.ValidString(s) &&
		strings.IndexFunc(s, func(r rune) bool { return !unicode.Is(messageChars, r) }) < 0
}

// CheckToken checks a MessageDeduplicationId, MessageGroupId or
// ReceiveRequestAttemptId.
func CheckToken(field, v string) error {
	if !tokenPattern.MatchString(v) {
		return errInvalidValue(field, v, "Must be 1 to 128 alphanumeric or punctuation characters")
	}
	return nil
}

// checkRange checks an optional integer parameter.
func checkRange(field string, v *int, lo, hi int) error {
	if v != nil && (*v < lo || *v > hi) {
		return errInvalidValue(field, strconv.Itoa(*v),
			fmt.Sprintf("Must be >= %d and <= %d, if provided", lo, hi))
	}
	return nil
}

// checkAttributeName checks a message attribute's name as the developer
// guide gives the rules.
func checkAttributeName(name string) error {
	lower := strings.ToLower(name)
	switch {
	case name == "" || len(name) > maxAttributeNameLen || !attributeNamePattern.MatchString(name):
		return ErrInvalidParameterValue("Message (user) attribute name '" + name + "' is " +
			"invalid. The name can contain alphanumeric characters, hyphens, underscores " +
			"and periods, up to 256 characters.")
	case strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon."):
		return ErrInvalidParameterValue("You can't use message attribute names beginning " +
			"with 'AWS.' or 'Amazon.'. These strings are reserved for internal use.")
	case strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") ||
		strings.Contains(name, ".."):
		return ErrInvalidParameterValue("Message (user) attribute name '" + name + "' " +
			"can't start or end with a period, or have two periods in a row.")
	}
	return nil
}

// checkNumber checks a Number attribute's value: up to 38 significant
// digits, between 10^-128 and 10^+126 in magnitude.
func checkNumber(name, v string) error {
	if !numberPattern.MatchString(v) {
		return ErrInvalidParameterValue("Can't cast the value of message (user) attribute '" +
			name + "' to a number.")
	}
	mantissa, expPart, _ := strings.Cut(strings.ToLower(strings.TrimLeft(v, "+-")), "e")
	exp := 0
	if expPart != "" {
		// The pattern holds the exponent to digits, so only its size can
		// fail to parse, and a size beyond int is out of range anyway.
		e, err := strconv.Atoi(expPart)
		if err != nil {
			return errNumberRange(name)
		}
		exp = e
	}
	intPart, frac, _ := strings.Cut(mantissa, ".")
	intPart = strings.TrimLeft(intPart, "0")
	digits := strings.Trim(intPart+frac, "0")
	if digits == "" {
		return nil
	}
	// The value is d.ddd x 10^magnitude.
	var magnitude int
	if intPart != "" {
		magnitude = exp + len(intPart) - 1
	} else {
		magnitude = exp - (len(frac) - len(strings.TrimLeft(frac, "0"))) - 1
	}
	if len(digits) > numberMaxDigits || magnitude > numberMaxExponent ||
		magnitude < numberMinExponent {
		return errNumberRange(name)
	}
	return nil
}

func errNumberRange(name string) *Error {
	return ErrInvalidParameterValue("Value of message (user) attribute '" + name +
		"' is out of range: a number has up to 38 digits, between 10^-128 and 10^+126.")
}

// contentDeduplicationID is the deduplication ID of a FIFO queue with
// content-based deduplication: the SHA-256 of the body, not of the
// attributes.
func contentDeduplicationID(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// checkAttributeValue checks one attribute's data type and value, and
// returns it for the store with the bytes it counts toward the message
// size: its name, data type and value.
func checkAttributeValue(kind, name string, v MessageAttributeValue) (
	contracts.MessageAttribute, int, error,
) {
	out := contracts.MessageAttribute{DataType: v.DataType}
	base, err := checkDataType(kind, name, v)
	if err != nil {
		return out, 0, err
	}
	size := len(name) + len(v.DataType)
	if base == typeBinary {
		if len(v.BinaryValue) == 0 || v.StringValue != nil {
			return out, 0, ErrInvalidParameterValue(fmt.Sprintf(
				"Message %s attribute '%s' with type 'Binary' must use field 'Binary'.",
				kind, name))
		}
		out.BinaryValue = v.BinaryValue
		return out, size + len(v.BinaryValue), nil
	}
	if err = checkStringValue(kind, name, base, v); err != nil {
		return out, 0, err
	}
	str := *v.StringValue
	out.StringValue = &str
	return out, size + len(str), nil
}

// checkDataType checks an attribute's data type, and that it uses none of
// the list values SQS reserves, and returns its base type.
func checkDataType(kind, name string, v MessageAttributeValue) (string, error) {
	if v.DataType == "" {
		return "", ErrInvalidParameterValue(fmt.Sprintf(
			"The message %s attribute '%s' must contain a non-empty message attribute type.",
			kind, name))
	}
	base, ok := baseOf(v.DataType)
	if !ok {
		return "", ErrInvalidParameterValue(fmt.Sprintf(
			"The type of message %s attribute '%s' is invalid. You must use only the "+
				"following supported type prefixes: Binary, Number, String.", kind, name))
	}
	if len(v.StringListValues) > 0 || len(v.BinaryListValues) > 0 {
		return "", ErrInvalidParameterValue(fmt.Sprintf(
			"Message %s attribute '%s' uses StringListValues or BinaryListValues, which "+
				"SQS reserves for future use.", kind, name))
	}
	return base, nil
}

// baseOf returns the base type of a data type, String, Number or Binary,
// reporting false for a data type SQS does not take: another base, an
// empty custom suffix, more than 256 characters, or a character a message
// may not hold.
func baseOf(dataType string) (string, bool) {
	base, custom, hasCustom := strings.Cut(dataType, ".")
	ok := base == typeString || base == typeNumber || base == typeBinary
	return base, ok && (!hasCustom || custom != "") && len(dataType) <= maxDataTypeLen &&
		validChars(dataType)
}

// checkStringValue checks the value of a String or Number attribute.
func checkStringValue(kind, name, base string, v MessageAttributeValue) error {
	if v.StringValue == nil || *v.StringValue == "" || len(v.BinaryValue) > 0 {
		return ErrInvalidParameterValue(fmt.Sprintf(
			"Message %s attribute '%s' must contain a non-empty value of message attribute "+
				"type '%s'.", kind, name, base))
	}
	if !validChars(*v.StringValue) {
		return ErrInvalidMessageContents()
	}
	if base == typeNumber {
		return checkNumber(name, *v.StringValue)
	}
	return nil
}

// checkMessageAttributes checks a message's attributes and returns them for
// the store, with the bytes they count toward the message size.
func checkMessageAttributes(attrs map[string]MessageAttributeValue) (
	map[string]contracts.MessageAttribute, int, error,
) {
	if len(attrs) == 0 {
		return nil, 0, nil
	}
	if len(attrs) > MaxMessageAttributes {
		return nil, 0, ErrInvalidParameterValue(fmt.Sprintf(
			"Number of message attributes [%d] exceeds the allowed maximum [%d].",
			len(attrs), MaxMessageAttributes))
	}
	out := make(map[string]contracts.MessageAttribute, len(attrs))
	total := 0
	for _, name := range sortedKeys(attrs) {
		if err := checkAttributeName(name); err != nil {
			return nil, 0, err
		}
		a, size, err := checkAttributeValue("(user)", name, attrs[name])
		if err != nil {
			return nil, 0, err
		}
		out[name] = a
		total += size
	}
	return out, total, nil
}

// checkSystemAttributes checks a message's system attributes: AWSTraceHeader
// is the only one SQS takes, as a String. They do not count toward the
// message size.
func checkSystemAttributes(attrs map[string]MessageAttributeValue) (
	map[string]contracts.MessageAttribute, error,
) {
	if len(attrs) == 0 {
		return nil, nil
	}
	out := make(map[string]contracts.MessageAttribute, len(attrs))
	for name, v := range attrs {
		if name != attrTraceHeader {
			return nil, ErrInvalidParameterValue("Message system attribute name '" + name +
				"' is invalid.")
		}
		if v.DataType != typeString {
			return nil, ErrInvalidParameterValue("Message system attribute '" + name +
				"' must have the data type String.")
		}
		a, _, err := checkAttributeValue("system", name, v)
		if err != nil {
			return nil, err
		}
		out[name] = a
	}
	return out, nil
}

// SendTarget is what a send is checked against: the queue's type and the
// attributes that bound its messages.
type SendTarget struct {
	MaximumMessageSize        int
	FIFO                      bool
	ContentBasedDeduplication bool
}

// SendFields are the fields of a SendMessage, or of one SendMessageBatch
// entry.
type SendFields struct {
	Attributes       map[string]MessageAttributeValue
	SystemAttributes map[string]MessageAttributeValue
	DelaySeconds     *int
	Body             *string
	DeduplicationID  *string
	GroupID          *string
}

// CheckSend checks a message against the queue it is sent to and returns it
// for the store, with its size: its body and attributes.
func CheckSend(f SendFields, q SendTarget) (contracts.SendInput, int, error) {
	var in contracts.SendInput
	if f.Body == nil || *f.Body == "" {
		return in, 0, ErrMissingParameter("MessageBody")
	}
	if !validChars(*f.Body) {
		return in, 0, ErrInvalidMessageContents()
	}
	if err := checkRange("DelaySeconds", f.DelaySeconds, 0, MaxDelaySeconds); err != nil {
		return in, 0, err
	}
	attrs, attrSize, err := checkMessageAttributes(f.Attributes)
	if err != nil {
		return in, 0, err
	}
	system, err := checkSystemAttributes(f.SystemAttributes)
	if err != nil {
		return in, 0, err
	}
	size := len(*f.Body) + attrSize
	if size > q.MaximumMessageSize {
		return in, 0, ErrInvalidParameterValue(fmt.Sprintf(
			"One or more parameters are invalid. Reason: Message must be shorter than %d bytes.",
			q.MaximumMessageSize))
	}
	if err = checkOrdering(f, q, &in); err != nil {
		return in, 0, err
	}
	in.Body = *f.Body
	in.Attributes = attrs
	in.SystemAttributes = system
	in.MD5OfBody = MD5OfBody(in.Body)
	in.MD5OfAttributes = MD5OfAttributes(attrs)
	in.MD5OfSystemAttributes = MD5OfAttributes(system)
	return in, size, nil
}

// checkOrdering checks the fields that depend on the queue's type, and sets
// them on in: the group, which a FIFO queue requires and a standard queue
// takes for fair queuing; the deduplication ID, a FIFO queue's own, the
// request's or the body's; and the delay, which only a standard queue takes
// per message.
func checkOrdering(f SendFields, q SendTarget, in *contracts.SendInput) error {
	if f.GroupID != nil {
		if err := CheckToken("MessageGroupId", *f.GroupID); err != nil {
			return err
		}
		in.GroupID = *f.GroupID
	}
	if f.DeduplicationID != nil {
		if err := CheckToken("MessageDeduplicationId", *f.DeduplicationID); err != nil {
			return err
		}
	}
	if !q.FIFO {
		if f.DeduplicationID != nil {
			return errInvalidValue("MessageDeduplicationId", *f.DeduplicationID,
				invalidForQueueType)
		}
		in.DelaySeconds = f.DelaySeconds
		return nil
	}
	return checkFIFO(f, q, in)
}

// checkFIFO checks a message to a FIFO queue: a group, no delay of its own,
// and a deduplication ID, the request's or, with content-based
// deduplication, the body's.
func checkFIFO(f SendFields, q SendTarget, in *contracts.SendInput) error {
	// A delay of 0 asks for nothing a FIFO queue does not do already.
	if f.DelaySeconds != nil && *f.DelaySeconds != 0 {
		return errInvalidValue("DelaySeconds", strconv.Itoa(*f.DelaySeconds),
			invalidForQueueType)
	}
	if f.GroupID == nil {
		return ErrMissingParameter("MessageGroupId")
	}
	switch {
	case f.DeduplicationID != nil:
		in.DeduplicationID = *f.DeduplicationID
	case q.ContentBasedDeduplication:
		in.DeduplicationID = contentDeduplicationID(*f.Body)
	default:
		return ErrInvalidParameterValue("The queue should either have " +
			"ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")
	}
	return nil
}

// CheckBatchIDs checks the entry IDs of a batch request: 1 to 10 entries,
// each ID well formed and distinct.
func CheckBatchIDs(ids []*string) error {
	switch {
	case len(ids) == 0:
		return errEmptyBatch()
	case len(ids) > MaxBatchEntries:
		return errTooManyEntries(len(ids))
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == nil {
			return ErrMissingParameter("Id")
		}
		if !batchIDPattern.MatchString(*id) {
			return errInvalidBatchEntryID(*id)
		}
		if seen[*id] {
			return errIDsNotDistinct(*id)
		}
		seen[*id] = true
	}
	return nil
}

// CheckBatchSize checks the total size of a SendMessageBatch's messages.
func CheckBatchSize(total int) error {
	if total > MaxMessageSize {
		return errBatchTooLong(total)
	}
	return nil
}

// Required returns a required string parameter, failing with
// MissingParameter when the request leaves it out or sends it empty.
func Required(field string, v *string) (string, error) {
	if v == nil || *v == "" {
		return "", ErrMissingParameter(field)
	}
	return *v, nil
}

// CheckReceive checks a ReceiveMessage's numbers and attempt ID.
func CheckReceive(req *ReceiveMessageRequest) error {
	if err := checkRange("MaxNumberOfMessages", req.MaxNumberOfMessages, 1,
		MaxReceiveMessages); err != nil {
		return err
	}
	if err := checkRange("VisibilityTimeout", req.VisibilityTimeout, 0,
		MaxVisibilityTimeout); err != nil {
		return err
	}
	if err := checkRange("WaitTimeSeconds", req.WaitTimeSeconds, 0,
		MaxWaitTimeSeconds); err != nil {
		return err
	}
	if req.ReceiveRequestAttemptID != nil {
		return CheckToken("ReceiveRequestAttemptId", *req.ReceiveRequestAttemptID)
	}
	return nil
}

// CheckVisibilityTimeout checks a ChangeMessageVisibility timeout, which is
// required.
func CheckVisibilityTimeout(v *int) (int, error) {
	if v == nil {
		return 0, ErrMissingParameter("VisibilityTimeout")
	}
	if err := checkRange("VisibilityTimeout", v, 0, MaxVisibilityTimeout); err != nil {
		return 0, err
	}
	return *v, nil
}

// CheckMaxResults checks a list operation's MaxResults, 1 to 1000. It
// returns 0 when the request sets none.
func CheckMaxResults(v *int) (int, error) {
	if v == nil {
		return 0, nil
	}
	if err := checkRange("MaxResults", v, 1, MaxListResults); err != nil {
		return 0, err
	}
	return *v, nil
}

// CheckMoveRate checks a StartMessageMoveTask's MaxNumberOfMessagesPerSecond,
// 1 to 500: "The maximum value for messages per second is 500" (API
// reference). nil asks for the system-optimized rate.
func CheckMoveRate(v *int) error {
	return checkRange("MaxNumberOfMessagesPerSecond", v, 1, MaxMoveRate)
}

// CheckMoveTaskResults checks a ListMessageMoveTasks's MaxResults, 1 to 10,
// and returns it, or 1, the default, when the request sets none: "The
// default is 1 [...]. The upper limit is 10" (API reference).
func CheckMoveTaskResults(v *int) (int, error) {
	if v == nil {
		return 1, nil
	}
	if err := checkRange("MaxResults", v, 1, MaxMoveTasksListed); err != nil {
		return 0, err
	}
	return *v, nil
}

// CheckQueueARN checks that a parameter is an SQS ARN in form,
// arn:<partition>:sqs:<region>:<account>:<name>, whichever queue it names.
func CheckQueueARN(field, arn string) error {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" ||
		slices.Contains(parts, "") {
		return errInvalidValue(field, arn, "Must be an Amazon SQS queue ARN")
	}
	return nil
}

// CheckTags checks tags as the developer guide's tag quotas give them: keys
// of 1 to 128 and values of up to 256 tag characters, no key with the
// reserved aws: prefix.
func CheckTags(tags map[string]string) error {
	for k, v := range tags {
		if k == "" || utf8.RuneCountInString(k) > maxTagKeyLen || !tagPattern.MatchString(k) {
			return errInvalidValue("Tag.Key", k, "Tag keys must be 1 to 128 characters of "+
				"letters, numbers, whitespace and _ . : / = + - @")
		}
		if strings.HasPrefix(strings.ToLower(k), reservedTagPrefix) {
			return errInvalidValue("Tag.Key", k, "Tag keys can't start with aws:")
		}
		if utf8.RuneCountInString(v) > maxTagValueLen || !tagPattern.MatchString(v) {
			return errInvalidValue("Tag.Value", v, "Tag values must be up to 256 characters "+
				"of letters, numbers, whitespace and _ . : / = + - @")
		}
	}
	return nil
}
