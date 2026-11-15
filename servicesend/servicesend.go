// Package servicesend is the client of the queues service's service-send
// endpoint, through which a Tule service sends a message to a queue as an
// AWS service principal, as Amazon S3 sends an event notification to a
// queue as s3.amazonaws.com: for a queue whose queue policy allows that
// service sqs:SendMessage.
//
// The endpoint is not an AWS API. It is a SigV4-signed POST, with the root
// key for the signing name sqs, of a JSON request to Path. It evaluates the
// queue's policy for the service principal, and no identity policies,
// since a service principal has none, and sends the message as SendMessage
// sends one.
package servicesend

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // SQS's digest of the body, not a security measure
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Path is where the queues service serves the service-send endpoint.
const Path = "/_internal/send-message"

// SigningName is the service a call is signed for, SQS's.
const SigningName = "sqs"

// RequestIDHeader is the response header that carries the call's request
// ID, SQS's.
const RequestIDHeader = "X-Amzn-RequestId"

// Request is the body of a call. QueueArn is the ARN of the queue the
// message is sent to. ServicePrincipal is the service the message is sent
// as, such as s3.amazonaws.com. SourceArn and SourceAccount, when set,
// name the resource the service acts for and its account, such as a bucket
// and its owner, and are supplied to the queue policy as aws:SourceArn and
// aws:SourceAccount, as AWS services supply them against the confused
// deputy problem (the IAM User Guide, the confused deputy problem).
// MessageBody is the message, as SendMessage takes it.
type Request struct {
	QueueArn         string `json:"QueueArn"`
	ServicePrincipal string `json:"ServicePrincipal"`
	SourceArn        string `json:"SourceArn,omitempty"`
	SourceAccount    string `json:"SourceAccount,omitempty"`
	MessageBody      string `json:"MessageBody"`
}

// Response is the body of a successful call: the message's ID and the MD5
// digest of its body, as SendMessage answers them.
type Response struct {
	MessageID        string `json:"MessageId"`
	MD5OfMessageBody string `json:"MD5OfMessageBody"`
}

// ErrorResponse is the body of a failed call.
type ErrorResponse struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// The codes a call fails with. The endpoint's own authentication failures
// are 403 with the sigv4 Kind as the code, and a failure of the service is
// 500 InternalFailure.
const (
	// CodeResourceNotFound, 404: no queue has the ARN, or the ARN is of
	// another partition, region or account than the service's.
	CodeResourceNotFound = "ResourceNotFound"
	// CodeAccessDenied, 403: the queue policy does not allow the service
	// principal sqs:SendMessage.
	CodeAccessDenied = "AccessDenied"
	// CodeInvalidRequest, 400: the body is not a request, or a field is
	// malformed.
	CodeInvalidRequest = "InvalidRequest"
	// CodeInvalidMessage, 400: the queue refuses the message as SendMessage
	// refuses it: an empty body, characters SQS does not take, a body over
	// the queue's MaximumMessageSize, or a FIFO queue, which needs a
	// message group.
	CodeInvalidMessage = "InvalidMessage"
	// CodeRequestTooLarge, 413: the request is over the endpoint's bound.
	CodeRequestTooLarge = "RequestTooLarge"
)

// The errors the client returns for the refusals a caller can act on.
var (
	ErrNotFound       = errors.New("servicesend: no such queue")
	ErrAccessDenied   = errors.New("servicesend: the queue policy does not allow the service")
	ErrInvalidMessage = errors.New("servicesend: the queue refuses the message")
)

// Error is any other failed call.
type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string {
	return fmt.Sprintf("servicesend: the service-send endpoint answered %d %s: %s",
		e.Status, e.Code, e.Message)
}

// Result is what a successful call returns. RequestID is the call's
// request ID, from the response's X-Amzn-RequestId. MessageID and
// MD5OfMessageBody are the message's, as SendMessage answers them; the
// digest is checked against the body sent.
type Result struct {
	RequestID        string
	MessageID        string
	MD5OfMessageBody string
}

// Option sets an optional field of a SendMessage request.
type Option func(*Request)

// WithSourceArn supplies arn, the resource the service acts for, as
// aws:SourceArn.
func WithSourceArn(arn string) Option {
	return func(r *Request) { r.SourceArn = arn }
}

// WithSourceAccount supplies account, the account of the resource the
// service acts for, as aws:SourceAccount.
func WithSourceAccount(account string) Option {
	return func(r *Request) { r.SourceAccount = account }
}

// Config configures a Client.
type Config struct {
	// HTTPClient makes the calls. It defaults to http.DefaultClient, which
	// sets no timeout.
	HTTPClient *http.Client
	// Endpoint is the queues service's base URL, such as
	// http://localhost:9050.
	Endpoint string
	// Region is the queues service's region, which the calls are signed
	// for, such as us-east-1.
	Region string
	// AccessKeyID and SecretAccessKey are the root key, which the calls are
	// signed with.
	AccessKeyID     string
	SecretAccessKey string
}

// Client calls the service-send endpoint. It is safe for concurrent use.
type Client struct {
	http   *http.Client
	signer *v4.Signer
	creds  aws.Credentials
	base   string
	region string
}

// New returns a Client for cfg. It fails when the endpoint is not an
// absolute http or https URL, or the region or the key is not set.
func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("servicesend: the endpoint %q is not an http or https URL",
			cfg.Endpoint)
	}
	if cfg.Region == "" {
		return nil, errors.New("servicesend: the region is required")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, errors.New("servicesend: the root access key ID and secret are required")
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{
		http:   hc,
		signer: v4.NewSigner(),
		creds: aws.Credentials{
			AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey,
		},
		base:   strings.TrimSuffix(u.String(), "/"),
		region: cfg.Region,
	}, nil
}

// maxResponseSize bounds what is read of a response, which is a message ID
// and a digest, or an error document.
const maxResponseSize = 64 << 10

// SendMessage sends body to the queue queueARN names as servicePrincipal,
// with the options set. The queue receives body exactly: a body that is
// not UTF-8, which JSON could carry only as other characters, is an error
// before anything is sent. It returns ErrNotFound, ErrAccessDenied or
// ErrInvalidMessage for those refusals, and an *Error or a transport error
// otherwise.
func (c *Client) SendMessage(
	ctx context.Context,
	queueARN, servicePrincipal, body string,
	opts ...Option,
) (*Result, error) {
	if !utf8.ValidString(body) {
		return nil, errors.New("servicesend: the message body is not UTF-8")
	}
	req := Request{QueueArn: queueARN, ServicePrincipal: servicePrincipal}
	for _, o := range opts {
		o(&req)
	}
	// The body is SendMessage's argument alone.
	if req.MessageBody != "" {
		return nil, errors.New("servicesend: an option set the MessageBody; " +
			"pass the body to SendMessage")
	}
	req.MessageBody = body
	raw, err := encode(req)
	if err != nil {
		return nil, fmt.Errorf("servicesend: encode the request: %w", err)
	}
	status, header, out, err := c.post(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("servicesend: %w", err)
	}
	if status != http.StatusOK {
		return nil, decodeError(status, out)
	}
	var res Response
	if err = json.Unmarshal(out, &res); err != nil {
		return nil, fmt.Errorf("servicesend: decode the response: %w", err)
	}
	sum := md5.Sum([]byte(body)) //nolint:gosec // SQS's digest of the body
	if want := hex.EncodeToString(sum[:]); res.MD5OfMessageBody != want {
		return nil, fmt.Errorf("servicesend: the queue's digest of the body is %q, "+
			"not the body's %q", res.MD5OfMessageBody, want)
	}
	return &Result{
		RequestID:        header.Get(RequestIDHeader),
		MessageID:        res.MessageID,
		MD5OfMessageBody: res.MD5OfMessageBody,
	}, nil
}

// encode encodes req as JSON without escaping <, > and &, which
// encoding/json escapes by default as six bytes each, so a body near the
// queue's limit is not sent as one several times longer. The body
// decodes to the same characters either way.
func encode(req Request) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(req); err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the caller
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// post posts body to Path, signed, and returns the response's status,
// header and body. A body over maxResponseSize is an error that says so,
// rather than one cut short.
func (c *Client) post(ctx context.Context, body []byte) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+Path,
		bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("build the request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	sum := sha256.Sum256(body)
	if err = c.signer.SignHTTP(ctx, c.creds, req, hex.EncodeToString(sum[:]),
		SigningName, c.region, time.Now()); err != nil {
		return 0, nil, nil, fmt.Errorf("sign the request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("call %s: %w", Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read the response: %w", err)
	}
	if len(raw) > maxResponseSize {
		return 0, nil, nil, fmt.Errorf("the response exceeds %d bytes", maxResponseSize)
	}
	return resp.StatusCode, resp.Header, raw, nil
}

func decodeError(status int, raw []byte) error {
	var e ErrorResponse
	if err := json.Unmarshal(raw, &e); err != nil {
		return &Error{Status: status, Message: string(raw)}
	}
	switch {
	case status == http.StatusNotFound && e.Code == CodeResourceNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, e.Message)
	case status == http.StatusForbidden && e.Code == CodeAccessDenied:
		return fmt.Errorf("%w: %s", ErrAccessDenied, e.Message)
	case status == http.StatusBadRequest && e.Code == CodeInvalidMessage:
		return fmt.Errorf("%w: %s", ErrInvalidMessage, e.Message)
	}
	return &Error{Status: status, Code: e.Code, Message: e.Message}
}
