package sqsapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/httpx"
)

// Error is an SQS error. It implements error so handlers and the validation
// helpers can return one directly, and is rendered in whichever protocol the
// request spoke.
//
// Code is the error code SQS's Query protocol sends, which the AWS JSON
// protocol carries too, in the x-amzn-query-error header, because the SQS
// model has the awsQueryCompatible trait: the SDKs report it as the error
// code. Shape is the name of the error shape in the SQS model, the JSON
// protocol's __type, or "" for the errors common to the AWS services, which
// the SQS model has no shape for.
type Error struct {
	Code    string
	Shape   string
	Message string
	Status  int
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

// Sender reports whether the error is the caller's fault: every error below
// 500. It is the fault the Query error document's Type and the
// x-amzn-query-error header carry.
func (e *Error) Sender() bool {
	return e.Status < http.StatusInternalServerError
}

// The error shapes of the SQS model this service raises, with the Query
// codes and statuses their awsQueryError traits give
// (aws-sdk-go-v2/service/sqs schemas). The shapes without the trait,
// InvalidAttributeName, InvalidAttributeValue, InvalidMessageContents and
// MessageNotInflight's peers, keep the shape name as the code and take 400,
// the status every per-action error has in the API reference.
var (
	shapeQueueDoesNotExist = shape("QueueDoesNotExist",
		"AWS.SimpleQueueService.NonExistentQueue", http.StatusBadRequest)
	shapeQueueNameExists = shape(
		"QueueNameExists",
		"QueueAlreadyExists",
		http.StatusBadRequest,
	)
	shapeQueueDeletedRecently = shape("QueueDeletedRecently",
		"AWS.SimpleQueueService.QueueDeletedRecently", http.StatusBadRequest)
	shapePurgeQueueInProgress = shape("PurgeQueueInProgress",
		"AWS.SimpleQueueService.PurgeQueueInProgress", http.StatusForbidden)
	shapeReceiptHandleIsInvalid = shape("ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid",
		http.StatusNotFound)
	shapeMessageNotInflight = shape("MessageNotInflight",
		"AWS.SimpleQueueService.MessageNotInflight", http.StatusBadRequest)
	shapeInvalidAttributeName   = shape("InvalidAttributeName", "InvalidAttributeName", 400)
	shapeInvalidAttributeValue  = shape("InvalidAttributeValue", "InvalidAttributeValue", 400)
	shapeInvalidMessageContents = shape("InvalidMessageContents", "InvalidMessageContents",
		http.StatusBadRequest)
	shapeInvalidAddress = shape("InvalidAddress", "InvalidAddress", http.StatusNotFound)
	shapeOverLimit      = shape("OverLimit", "OverLimit", http.StatusForbidden)
	shapeEmptyBatch     = shape(
		"EmptyBatchRequest",
		"AWS.SimpleQueueService.EmptyBatchRequest",
		400,
	)
	shapeTooManyEntries = shape("TooManyEntriesInBatchRequest",
		"AWS.SimpleQueueService.TooManyEntriesInBatchRequest", http.StatusBadRequest)
	shapeIDsNotDistinct = shape("BatchEntryIdsNotDistinct",
		"AWS.SimpleQueueService.BatchEntryIdsNotDistinct", http.StatusBadRequest)
	shapeInvalidBatchEntryID = shape("InvalidBatchEntryId",
		"AWS.SimpleQueueService.InvalidBatchEntryId", http.StatusBadRequest)
	shapeBatchTooLong = shape("BatchRequestTooLong",
		"AWS.SimpleQueueService.BatchRequestTooLong", http.StatusBadRequest)
	shapeUnsupportedOperation = shape("UnsupportedOperation",
		"AWS.SimpleQueueService.UnsupportedOperation", http.StatusBadRequest)
)

type errorShape struct {
	name   string
	code   string
	status int
}

func shape(name, code string, status int) errorShape {
	return errorShape{name: name, code: code, status: status}
}

func (s errorShape) err(msg string) *Error {
	return &Error{Code: s.code, Shape: s.name, Message: msg, Status: s.status}
}

// The codes common to the AWS services this service raises, with the
// statuses of the SQS common errors reference.
const (
	CodeInvalidParameterValue = "InvalidParameterValue"
	CodeMissingParameter      = "MissingParameter"
	CodeInvalidAction         = "InvalidAction"
	CodeMissingAction         = "MissingAction"
	CodeMalformedQueryString  = "MalformedQueryString"
	CodeInternalFailure       = "InternalFailure"
	CodeMissingAuthToken      = "MissingAuthenticationToken"
	CodeIncompleteSignature   = "IncompleteSignature"
	CodeSignatureDoesNotMatch = "SignatureDoesNotMatch"
	CodeInvalidClientTokenID  = "InvalidClientTokenId"
	CodeExpiredToken          = "ExpiredToken"
	CodeSerialization         = "SerializationException"
	CodeAccessDenied          = "AccessDenied"
)

// NewError builds a common error with the given status and code.
func NewError(status int, code, message string) *Error {
	return &Error{Status: status, Code: code, Message: message}
}

// ErrInvalidParameterValue is the error for a parameter whose value SQS
// does not accept, in the form of SQS's messages.
func ErrInvalidParameterValue(msg string) *Error {
	return NewError(http.StatusBadRequest, CodeInvalidParameterValue, msg)
}

// errInvalidValue is ErrInvalidParameterValue for the value of field, with
// the reason SQS gives.
func errInvalidValue(field, value, reason string) *Error {
	return ErrInvalidParameterValue(fmt.Sprintf(
		"Value %s for parameter %s is invalid. Reason: %s.", value, field, reason))
}

// ErrMissingParameter is the error for a required parameter the request
// leaves out.
func ErrMissingParameter(field string) *Error {
	return NewError(http.StatusBadRequest, CodeMissingParameter,
		"The request must contain the parameter "+field+".")
}

// ErrInvalidAction is the error for an Action, or an X-Amz-Target, this
// service does not serve, whether or not SQS has it.
func ErrInvalidAction(op string) *Error {
	return NewError(http.StatusBadRequest, CodeInvalidAction,
		"The action "+op+" is not valid for this endpoint.")
}

// ErrMissingAction is the error for a Query request that names no Action.
func ErrMissingAction() *Error {
	return NewError(http.StatusBadRequest, CodeMissingAction,
		"The request must contain the parameter Action.")
}

// ErrMalformedQueryString is the error for a body that does not parse as a
// form, with the status of the common errors reference.
func ErrMalformedQueryString() *Error {
	return NewError(http.StatusNotFound, CodeMalformedQueryString,
		"The query string contains a syntax error.")
}

// ErrSerialization is the error for a JSON body that does not decode, or
// whose fields have the wrong JSON types.
func ErrSerialization() *Error {
	return NewError(http.StatusBadRequest, CodeSerialization,
		"The request body is not valid JSON, or a field has the wrong type.")
}

// ErrInternal is the error every unrecognized failure becomes.
func ErrInternal() *Error {
	return NewError(http.StatusInternalServerError, CodeInternalFailure,
		"The request processing has failed because of an unknown error, exception or failure.")
}

// ErrAccessDenied is the error for a request the caller's identity policies
// do not allow: 403 AccessDenied, the code SQS sends in Query and in
// x-amzn-query-error ("Troubleshoot an access denied error in Amazon SQS",
// SQS Developer Guide), which the SQS model has no shape for. The message
// is IAM's form, with the action in lower case as SQS writes it in its
// AccessDenied messages (as reported by SQS users; no AWS page gives an
// identity-policy denial's exact text).
func ErrAccessDenied(d *authz.Denied) *Error {
	return NewError(http.StatusForbidden, CodeAccessDenied,
		authz.Message(d.Principal, strings.ToLower(d.Action), d.Resource, d.Reason))
}

// ErrQueueDoesNotExist is the error for a queue URL or name no queue has.
func ErrQueueDoesNotExist() *Error {
	return shapeQueueDoesNotExist.err("The specified queue does not exist.")
}

// ErrInvalidAddress is the error for a QueueUrl that is not a queue URL.
func ErrInvalidAddress(queueURL string) *Error {
	return shapeInvalidAddress.err("The address " + queueURL + " is not valid for this endpoint.")
}

// ErrReceiptHandleIsInvalid is the error for a receipt handle that names no
// receive of a message of the queue.
func ErrReceiptHandleIsInvalid(handle string) *Error {
	return shapeReceiptHandleIsInvalid.err(
		"The input receipt handle \"" + handle + "\" is not a valid receipt handle.")
}

// ErrInvalidAttributeName is the error for a queue attribute name SQS does
// not have, or that the queue or operation does not take.
func ErrInvalidAttributeName(name string) *Error {
	return shapeInvalidAttributeName.err("Unknown Attribute " + name + ".")
}

// ErrInvalidAttributeValue is the error for a queue attribute value out of
// its range or format.
func ErrInvalidAttributeValue(name, reason string) *Error {
	msg := "Invalid value for the parameter " + name + "."
	if reason != "" {
		msg += " Reason: " + reason
	}
	return shapeInvalidAttributeValue.err(msg)
}

// ErrInvalidMessageContents is the error for a body or attribute value with
// a character SQS does not allow.
func ErrInvalidMessageContents() *Error {
	return shapeInvalidMessageContents.err(
		"Invalid characters found. Valid unicode characters are #x9 | #xA | #xD | " +
			"#x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF")
}

// ErrUnsupportedOperation is the error for an operation the queue's type
// does not support.
func ErrUnsupportedOperation(msg string) *Error {
	return shapeUnsupportedOperation.err(msg)
}

// ErrOverLimit is the error for a request that would exceed a fixed quota.
func ErrOverLimit(msg string) *Error {
	return shapeOverLimit.err(msg)
}

// The batch errors.
func errEmptyBatch() *Error {
	return shapeEmptyBatch.err("There should be at least one entry in the request.")
}

func errTooManyEntries(n int) *Error {
	return shapeTooManyEntries.err(fmt.Sprintf(
		"Maximum number of entries per request are %d. You have sent %d.", MaxBatchEntries, n))
}

func errIDsNotDistinct(id string) *Error {
	return shapeIDsNotDistinct.err("Id " + id + " repeated.")
}

func errInvalidBatchEntryID(id string) *Error {
	return shapeInvalidBatchEntryID.err("A batch entry id can only contain alphanumeric " +
		"characters, hyphens and underscores. It can be at most 80 letters long. Got " + id + ".")
}

func errBatchTooLong(n int) *Error {
	return shapeBatchTooLong.err(fmt.Sprintf(
		"Batch requests cannot be longer than %d bytes. You have sent %d bytes.",
		MaxMessageSize, n))
}

// errRepeatedTagKey is the error for a request that gives one tag key two
// values. SQS documents no answer for it (TagQueue, CreateQueue); refusing
// it keeps the value stored and the value a policy's aws:RequestTag sees
// the same one.
func errRepeatedTagKey(key string) *Error {
	return errInvalidValue("Tag.Key", key, "The tag key is repeated")
}

// ErrTagKeyCaseCollision is the error for a tag key that differs only by
// case from another key of the request or, for TagQueue, of the queue, sent
// by a principal evaluated against its policies: condition keys match
// without regard to case, so the two would feed one aws:RequestTag or
// aws:ResourceTag key. It is errRepeatedTagKey's error, with its reason.
func ErrTagKeyCaseCollision(key string) *Error {
	return errInvalidValue("Tag.Key", key,
		"The tag key differs only by case from another tag key")
}

// sentinelErrors maps the store's sentinels to the errors they become.
var sentinelErrors = []struct {
	err    error
	apiErr *Error
}{
	{contracts.ErrQueueNotFound, ErrQueueDoesNotExist()},
	{contracts.ErrQueueExists, shapeQueueNameExists.err(
		"A queue already exists with the same name and a different value for attribute(s).")},
	{contracts.ErrQueueDeletedRecently, shapeQueueDeletedRecently.err(
		"You must wait 60 seconds after deleting a queue before you can create another " +
			"queue with the same name.")},
	{contracts.ErrPurgeInProgress, shapePurgeQueueInProgress.err(
		"Only one PurgeQueue operation on the queue is allowed every 60 seconds.")},
	{contracts.ErrMessageNotInflight, shapeMessageNotInflight.err(
		"The message referred to isn't in flight.")},
	{contracts.ErrInvalidToken, ErrInvalidParameterValue("Invalid NextToken value.")},
}

// ToError maps err to an SQS error, logging it when it maps to an internal
// error. It returns a copy, safe for the caller to fill in.
func ToError(ctx context.Context, err error) *Error {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		cp := *apiErr
		return &cp
	}
	for _, se := range sentinelErrors {
		if errors.Is(err, se.err) {
			cp := *se.apiErr
			return &cp
		}
	}
	var denied *authz.Denied
	if errors.As(err, &denied) {
		return ErrAccessDenied(denied)
	}
	var tagCase *contracts.TagKeyCaseError
	if errors.As(err, &tagCase) {
		return ErrTagKeyCaseCollision(tagCase.Key)
	}
	var receipt *contracts.ReceiptError
	if errors.As(err, &receipt) {
		return ErrReceiptHandleIsInvalid(receipt.Handle)
	}
	var invalid *contracts.InvalidError
	if errors.As(err, &invalid) {
		return errInvalidValue(invalid.Field, invalid.Value, invalid.Reason)
	}

	httpx.Logger(ctx).Error("request failed").Err("error", err).Send()
	return ErrInternal()
}
