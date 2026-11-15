package queues

// The service-send endpoint, through which a Tule service sends a message
// to a queue as an AWS service principal, as Amazon S3 sends an event
// notification as s3.amazonaws.com. It is not an AWS API;
// tule.dev/queues/servicesend is its client and holds its wire types.
//
// The app verifies every call to it, before this module reads anything,
// against the root key alone, signed for sqs, and answers a failure with
// WriteServiceSendError.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/gasmod/gas"
	"tule.dev/identity/servicerole"
	"tule.dev/sigv4"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/httpx"
	"tule.dev/queues/internal/sqsapi"
	"tule.dev/queues/servicesend"
)

// codeInternalFailure is the endpoint's code for a failure of the service,
// identity's internal endpoints' too.
const codeInternalFailure = "InternalFailure"

// refusal is a call the endpoint refuses, with the status and code of its
// answer.
type refusal struct {
	code    string
	message string
	status  int
}

func (r *refusal) Error() string { return r.code + ": " + r.message }

func invalidService(msg string) *refusal {
	return &refusal{
		status: http.StatusBadRequest, code: servicesend.CodeInvalidRequest, message: msg,
	}
}

func noServiceQueue(arn string) *refusal {
	return &refusal{
		status: http.StatusNotFound, code: servicesend.CodeResourceNotFound,
		message: "The specified queue does not exist: " + arn,
	}
}

// serviceRequest is the body of a call as it is decoded: MessageBody is
// kept as the JSON received, so it is decoded once, strictly
// (messageBody).
type serviceRequest struct {
	servicesend.Request
	MessageBody json.RawMessage `json:"MessageBody"`
}

// serviceSend sends the message the request carries to the queue its ARN
// names, as SendMessage sends one, when the queue's policy allows the
// service principal sqs:SendMessage on the queue. Only the queue's policy
// is evaluated, as a service principal has no identity policies. The
// message's SenderId is the service principal.
func (m *Module) serviceSend(ctx gas.Context) error {
	raw, err := io.ReadAll(ctx.Request().Body)
	if err != nil {
		return writeServiceFailure(ctx, fmt.Errorf("read the request body: %w", err))
	}
	// encoding/json would decode bytes that are not UTF-8 as U+FFFD, so the
	// queue would get other characters than were sent.
	if !utf8.Valid(raw) {
		return writeServiceRefusal(ctx, invalidService("The body is not UTF-8."))
	}
	var req serviceRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		return writeServiceRefusal(ctx, invalidService(
			"The body is not a JSON service-send request."))
	}
	body, lone, r := messageBody(req.MessageBody)
	if r != nil {
		return writeServiceRefusal(ctx, r)
	}
	req.Request.MessageBody = body
	if r = checkServiceRequest(&req.Request); r != nil {
		return writeServiceRefusal(ctx, r)
	}
	name, r := m.serviceQueue(req.QueueArn)
	if r != nil {
		return writeServiceRefusal(ctx, r)
	}
	res, err := m.sendAsService(ctx, &req.Request, name, lone)
	if err != nil {
		return writeServiceFailure(ctx, err)
	}
	return writeService(ctx, http.StatusOK, res)
}

// messageBody decodes the MessageBody member as it was received. It is a
// JSON string, or left out or null for none. encoding/json decodes an
// escaped lone surrogate, \ud800, as U+FFFD, a character SQS takes, so the
// queue would get another body than was sent; lone reports one, and the
// message is refused as having invalid characters once it is authorized.
func messageBody(raw json.RawMessage) (string, bool, *refusal) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false, nil
	}
	var body string
	if raw[0] != '"' || json.Unmarshal(raw, &body) != nil {
		return "", false, invalidService("MessageBody is not a string.")
	}
	return body, hasLoneSurrogate(raw), nil
}

// hasLoneSurrogate reports whether a JSON string escapes a UTF-16
// surrogate as \uXXXX other than as the first half of a pair followed by
// the second.
func hasLoneSurrogate(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++ // the escaped character, such as \\, \" or the u of \uXXXX
		if i < len(raw) && raw[i] == 'u' {
			var lone bool
			if i, lone = unicodeEscape(raw, i); lone {
				return true
			}
		}
	}
	return false
}

// unicodeEscape reads the \uXXXX escape whose u is raw[at], and the escape
// after it when it is the first half of a surrogate pair. It returns the
// index of the last byte read, and whether the escape is a lone surrogate.
func unicodeEscape(raw []byte, at int) (int, bool) {
	r, ok := hex4(raw, at+1)
	if !ok {
		return at, false // malformed, which json.Unmarshal refused already
	}
	end := at + 4
	switch {
	case isLowSurrogate(r):
		return end, true
	case !isHighSurrogate(r):
		return end, false
	case end+2 >= len(raw) || raw[end+1] != '\\' || raw[end+2] != 'u':
		return end, true
	}
	low, ok := hex4(raw, end+3)
	return end + 6, !ok || !isLowSurrogate(low)
}

func isHighSurrogate(r uint64) bool { return r >= 0xD800 && r <= 0xDBFF }

func isLowSurrogate(r uint64) bool { return r >= 0xDC00 && r <= 0xDFFF }

// hex4 reads the four hexadecimal digits of a \u escape at raw[at:].
func hex4(raw []byte, at int) (uint64, bool) {
	if at+4 > len(raw) {
		return 0, false
	}
	v, err := strconv.ParseUint(string(raw[at:at+4]), 16, 32)
	return v, err == nil
}

// checkServiceRequest checks the request's fields: the service principal,
// SourceArn and SourceAccount as identity's assume-role endpoint checks
// them.
func checkServiceRequest(req *servicesend.Request) *refusal {
	if !servicerole.IsServicePrincipal(req.ServicePrincipal) {
		return invalidService("ServicePrincipal " + strconv.Quote(req.ServicePrincipal) +
			" is not a service principal such as s3.amazonaws.com.")
	}
	if req.SourceArn != "" && !servicerole.IsSourceArn(req.SourceArn) {
		return invalidService("SourceArn " + strconv.Quote(req.SourceArn) + " is not an ARN.")
	}
	// An ARN may be 2048 characters, and a condition value is evaluated up
	// to the authorizer's cap.
	if len(req.SourceArn) > authz.MaxValueLength {
		return invalidService(fmt.Sprintf("SourceArn is %d characters; it must be at most %d.",
			len(req.SourceArn), authz.MaxValueLength))
	}
	if req.SourceAccount != "" && !servicerole.IsAccountID(req.SourceAccount) {
		return invalidService("SourceAccount " + strconv.Quote(req.SourceAccount) +
			" is not a 12-digit account ID.")
	}
	return nil
}

// serviceQueue returns the name of the queue arn names. A string that is
// no SQS ARN is malformed. An ARN of another partition, region or account,
// or one whose name no queue can have, names no queue of this service, as
// queueOfARN answers it.
func (m *Module) serviceQueue(arn string) (string, *refusal) {
	if _, _, _, _, err := sqsapi.ParseQueueARN(arn); err != nil {
		return "", invalidService("QueueArn " + strconv.Quote(arn) +
			" is not an Amazon SQS queue ARN.")
	}
	name, ok := m.settings.NameOfARN(arn)
	if !ok {
		return "", noServiceQueue(arn)
	}
	if _, err := sqsapi.CheckQueueName(name); err != nil {
		return "", noServiceQueue(arn)
	}
	return name, nil
}

// sendAsService authorizes the send and sends the message, pinned to the
// queue authorized (authorizeService). A queue deleted and created again
// under its name in between is not sent to, and is answered as not found.
func (m *Module) sendAsService(
	ctx gas.Context, req *servicesend.Request, name string, lone bool,
) (*servicesend.Response, error) {
	pinned, err := m.authorizeService(ctx, req, name)
	if err != nil {
		return nil, err
	}
	info, err := m.store.QueueInfo(pinned, name)
	if errors.Is(err, contracts.ErrQueueNotFound) {
		return nil, noServiceQueue(req.QueueArn)
	}
	if err != nil {
		return nil, err //nolint:wrapcheck // the store's error, logged by writeServiceFailure
	}
	in, err := checkServiceMessage(info, req.MessageBody, lone)
	if err != nil {
		return nil, err
	}
	in.SenderID = req.ServicePrincipal
	res, err := m.store.SendMessages(pinned, name, []contracts.SendInput{in})
	if errors.Is(err, contracts.ErrQueueNotFound) {
		return nil, noServiceQueue(req.QueueArn)
	}
	if err != nil {
		return nil, err //nolint:wrapcheck // the store's error, logged by writeServiceFailure
	}
	return &servicesend.Response{
		MessageID: res[0].MessageID, MD5OfMessageBody: in.MD5OfBody,
	}, nil
}

// authorizeService returns ctx pinned to the queue name when its policy
// allows the service principal sqs:SendMessage on it. The queue's tags and
// policy are read with its ID, as an SQS operation reads them (readQueue),
// so the policy evaluated is the one of the queue sent to. A queue whose
// policy cannot be evaluated, or whose tag keys differ only by case, is a
// fault, as it is for every evaluated request.
func (m *Module) authorizeService(
	ctx gas.Context, req *servicesend.Request, name string,
) (context.Context, error) {
	q, err := m.store.QueueAccess(ctx, name)
	if errors.Is(err, contracts.ErrQueueNotFound) {
		return nil, noServiceQueue(req.QueueArn)
	}
	if err != nil {
		return nil, err //nolint:wrapcheck // the store's error, logged by writeServiceFailure
	}
	if _, err = storedTagKeys(name, q.Tags); err != nil {
		return nil, err
	}
	pol, err := m.authorizer.ResourcePolicy(q.Policy)
	if err != nil {
		return nil, fmt.Errorf("authorize: queue %s: %w", name, err)
	}
	keys := map[string][]string{}
	// The resource the service acts for, as a service supplies it.
	if req.SourceArn != "" {
		keys["aws:SourceArn"] = []string{req.SourceArn}
	}
	if req.SourceAccount != "" {
		keys["aws:SourceAccount"] = []string{req.SourceAccount}
	}
	err = m.authorizer.AuthorizeService(ctx.Request(), req.ServicePrincipal, authz.Check{
		Action: sendMessageAction, Resource: m.settings.ARN(name), Policy: pol,
		Context: withResourceTags(keys, q.Tags),
	})
	var denied *authz.Denied
	if errors.As(err, &denied) {
		return nil, &refusal{
			status: http.StatusForbidden, code: servicesend.CodeAccessDenied,
			message: sqsapi.ErrAccessDenied(denied).Message,
		}
	}
	if err != nil {
		return nil, err //nolint:wrapcheck // a fault, logged by writeServiceFailure
	}
	return contracts.WithQueuePin(ctx, name, q.ID), nil
}

// checkServiceMessage checks body against the queue as SendMessage checks
// a message with no attributes, group or delay, and refuses one SendMessage
// refuses as InvalidMessage, with SendMessage's message. The sizes are
// those of the body's characters in UTF-8, as SQS measures a message, not
// of its escaped form in the request. A lone surrogate is refused as the
// invalid character it is.
func checkServiceMessage(q *contracts.Queue, body string, lone bool) (
	contracts.SendInput, error,
) {
	invalid := func(e *sqsapi.Error) *refusal {
		return &refusal{
			status: http.StatusBadRequest, code: servicesend.CodeInvalidMessage,
			message: e.Message,
		}
	}
	if lone {
		return contracts.SendInput{}, invalid(sqsapi.ErrInvalidMessageContents())
	}
	t, err := target(q)
	if err != nil {
		return contracts.SendInput{}, err
	}
	in, _, err := sqsapi.CheckSend(sqsapi.SendFields{Body: &body}, t)
	var apiErr *sqsapi.Error
	if errors.As(err, &apiErr) {
		return contracts.SendInput{}, invalid(apiErr)
	}
	if err != nil {
		return contracts.SendInput{}, err //nolint:wrapcheck // a fault, logged by writeServiceFailure
	}
	return in, nil
}

// writeServiceFailure answers a refusal with its code, and any other
// failure as internal, logged.
func writeServiceFailure(ctx gas.Context, err error) error {
	var r *refusal
	if errors.As(err, &r) {
		return writeServiceRefusal(ctx, r)
	}
	httpx.Logger(ctx).Error("service send failed").Err("error", err).Send()
	return writeService(ctx, http.StatusInternalServerError, servicesend.ErrorResponse{
		Code: codeInternalFailure, Message: "The message could not be sent.",
	})
}

func writeServiceRefusal(ctx gas.Context, r *refusal) error {
	return writeService(ctx, r.status, servicesend.ErrorResponse{
		Code: r.code, Message: r.message,
	})
}

// writeService answers v as JSON.
func writeService(ctx gas.Context, status int, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode the response: %w", err)
	}
	writeServiceBody(ctx.ResponseWriter(), ctx.Request(), status, body)
	return nil
}

// writeServiceBody sends body with the request's own ID, the one the
// request is logged under.
func writeServiceBody(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	if id := httpx.RequestIDOf(r.Context()); id != "" {
		w.Header().Set(servicesend.RequestIDHeader, id)
	}
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		httpx.Logger(r.Context()).Warn("failed to write response").Err("error", err).Send()
	}
}

// WriteServiceSendError answers a call to the service-send endpoint that
// failed verification, as identity's internal endpoints answer one: 403
// with the sigv4 Kind as the code and the verifier's message, and a failed
// key lookup as a 500 InternalFailure, logged. A body over the verifier's
// bound is 413 RequestTooLarge.
func WriteServiceSendError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, msg := http.StatusInternalServerError, codeInternalFailure,
		"The request could not be served."
	var sigErr *sigv4.Error
	switch {
	case errors.As(err, &sigErr) && sigErr.Kind == sigv4.KindBodyTooLarge:
		status, code, msg = http.StatusRequestEntityTooLarge, servicesend.CodeRequestTooLarge,
			sigErr.Message
	case errors.As(err, &sigErr) && sigErr.Kind != sigv4.KindKeyLookupFailed:
		status, code, msg = http.StatusForbidden, sigErr.Kind.String(), sigErr.Message
	default:
		httpx.Logger(r.Context()).Error("request failed").Err("error", err).Send()
	}
	body, mErr := json.Marshal(servicesend.ErrorResponse{Code: code, Message: msg})
	if mErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeServiceBody(w, r, status, body)
}
