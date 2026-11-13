// Package queues serves the SQS API over both protocols SQS speaks: one
// handler for POST / and for POST to a queue URL's path, dispatching on the
// operation an AWS JSON request names in X-Amz-Target or a Query request
// names in Action.
package queues

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gasmod/gas"

	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

const moduleName = "tule/modules/queues"

// The routes: the AWS JSON protocol and account-level Query actions post to
// /, and Query actions on a queue post to its URL.
const (
	rootPath  = "/"
	queuePath = "/{account}/{queue}"
)

// Module registers the API routes and serves the operations behind them.
type Module struct {
	router   *gas.Router
	store    contracts.QueueProvider
	settings *contracts.QueueSettings
	handlers map[string]handler
}

// handler serves one operation and returns its result, nil for an operation
// that returns nothing. Returning an error leaves the response to the
// dispatcher, which renders it in the request's protocol.
type handler func(*call) (any, error)

// call is one request to an operation.
type call struct {
	gas.Context
	params sqsapi.Params
	// pathQueue is the queue the URL path names, for a Query request sent
	// to a queue URL.
	pathQueue *string
	op        string
	body      []byte
	proto     sqsapi.Protocol
}

// NewModule returns the constructor gas uses to build a Module serving the
// queues of settings' account from store.
func NewModule(settings *contracts.QueueSettings) func(
	router *gas.Router, store contracts.QueueProvider,
) *Module {
	return func(router *gas.Router, store contracts.QueueProvider) *Module {
		m := &Module{router: router, store: store, settings: settings}
		m.handlers = map[string]handler{
			"CreateQueue":                  m.createQueue,
			"DeleteQueue":                  m.deleteQueue,
			"GetQueueUrl":                  m.getQueueURL,
			"ListQueues":                   m.listQueues,
			"GetQueueAttributes":           m.getQueueAttributes,
			"SetQueueAttributes":           m.setQueueAttributes,
			"PurgeQueue":                   m.purgeQueue,
			"TagQueue":                     m.tagQueue,
			"UntagQueue":                   m.untagQueue,
			"ListQueueTags":                m.listQueueTags,
			"ListDeadLetterSourceQueues":   m.listDeadLetterSourceQueues,
			"AddPermission":                m.addPermission,
			"RemovePermission":             m.removePermission,
			"SendMessage":                  m.sendMessage,
			"SendMessageBatch":             m.sendMessageBatch,
			"ReceiveMessage":               m.receiveMessage,
			"DeleteMessage":                m.deleteMessage,
			"DeleteMessageBatch":           m.deleteMessageBatch,
			"ChangeMessageVisibility":      m.changeMessageVisibility,
			"ChangeMessageVisibilityBatch": m.changeMessageVisibilityBatch,
		}
		return m
	}
}

// Name identifies the module to gas.
func (m *Module) Name() string {
	return moduleName
}

// Init registers the module's routes.
func (m *Module) Init(context.Context) error {
	m.router.Handle(m, http.MethodPost, rootPath, m.dispatch)
	m.router.Handle(m, http.MethodPost, queuePath, m.dispatch)
	return nil
}

// Close releases nothing; the module holds no resources of its own.
func (m *Module) Close(context.Context) error {
	return nil
}

// dispatch serves the operation the request names. An operation this
// service does not serve, whether or not SQS has it, is an invalid action.
func (m *Module) dispatch(ctx gas.Context) error {
	r := ctx.Request()
	c := &call{Context: ctx, proto: sqsapi.ProtocolOf(r)}
	if q := ctx.Param("queue"); q != "" {
		if _, err := sqsapi.CheckQueueName(q); err != nil {
			return m.fail(c, err)
		}
		if ctx.Param("account") != m.settings.AccountID {
			return m.fail(c, sqsapi.ErrQueueDoesNotExist())
		}
		c.pathQueue = &q
	}
	read := c.readQuery
	if c.proto == sqsapi.JSON {
		read = c.readJSON
	}
	if err := read(); err != nil {
		return m.fail(c, err)
	}

	h, ok := m.handlers[c.op]
	if !ok {
		return m.fail(c, sqsapi.ErrInvalidAction(c.op))
	}
	result, err := h(c)
	if err != nil {
		return m.fail(c, err)
	}
	sqsapi.Write(ctx.ResponseWriter(), r, c.proto, c.op, result)
	return nil
}

// readJSON reads a JSON request: its operation from X-Amz-Target and its
// body.
func (c *call) readJSON() error {
	c.op = sqsapi.Operation(c.Header(sqsapi.TargetHeader))
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return fmt.Errorf("read the request body: %w", err)
	}
	c.body = body
	return nil
}

// readQuery reads a Query request: its form, and the operation its Action
// names, for the API version it names, when it names one.
func (c *call) readQuery() error {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return sqsapi.ErrMalformedQueryString()
	}
	c.params = sqsapi.Params(r.Form)
	c.op = r.Form.Get("Action")
	if c.op == "" {
		return sqsapi.ErrMissingAction()
	}
	if v, ok := r.Form["Version"]; ok && (len(v) == 0 || v[0] != sqsapi.APIVersion) {
		return sqsapi.ErrInvalidAction(c.op)
	}
	return nil
}

func (m *Module) fail(c *call, err error) error {
	sqsapi.WriteError(c.ResponseWriter(), c.Request(), c.proto, err)
	return nil
}

// decode reads the request's parameters into v, its operation's request
// struct. An empty JSON body decodes to the zero value, and unknown JSON
// fields are ignored, as the AWS JSON protocol expects.
func (c *call) decode(v any) error {
	if c.proto == sqsapi.Query {
		return c.params.Decode(v) //nolint:wrapcheck // an *sqsapi.Error, or a missing decoder
	}
	if len(c.body) == 0 {
		return nil
	}
	if err := json.Unmarshal(c.body, v); err != nil {
		return sqsapi.ErrSerialization()
	}
	return nil
}

// queueName returns the name of the queue a request's QueueUrl names, or,
// for a Query request without one, its URL path names. A QueueUrl must be a
// queue URL of this service's account, with any host, as SQS routes on the
// endpoint the request reached rather than the URL's host.
func (m *Module) queueName(c *call, queueURL *string) (string, error) {
	if queueURL == nil || *queueURL == "" {
		if c.pathQueue != nil {
			return *c.pathQueue, nil
		}
		return "", sqsapi.ErrMissingParameter("QueueUrl")
	}
	name, ok := m.nameInURL(*queueURL)
	if !ok {
		return "", sqsapi.ErrInvalidAddress(*queueURL)
	}
	if name == "" {
		return "", sqsapi.ErrQueueDoesNotExist()
	}
	return name, nil
}

// nameInURL returns the queue name in a queue URL, an http or https URL
// whose path is /<account>/<name>. It returns false for a string that is no
// queue URL, and "" for a queue URL that can name no queue of this account.
func (m *Module) nameInURL(queueURL string) (string, bool) {
	u, err := url.Parse(queueURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", false
	}
	account, name, ok := splitQueuePath(u.Path)
	if !ok {
		return "", false
	}
	if _, err = sqsapi.CheckQueueName(name); err != nil || account != m.settings.AccountID {
		return "", true
	}
	return name, true
}

// splitQueuePath splits a queue URL's path, /<account>/<name>.
func splitQueuePath(path string) (string, string, bool) {
	account, name, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	return account, name, ok && account != "" && name != "" && !strings.Contains(name, "/")
}

// QueueURL returns the URL of the queue name, as a request to r reaches it:
// the configured base URL, or the scheme and host r came in on, then the
// account and the name.
func QueueURL(settings *contracts.QueueSettings, r *http.Request, name string) string {
	base := strings.TrimSuffix(settings.BaseURL, "/")
	if base == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		base = scheme + "://" + r.Host
	}
	return base + "/" + settings.AccountID + "/" + name
}

func (m *Module) queueURL(c *call, name string) string {
	return QueueURL(m.settings, c.Request(), name)
}

// queueURLs returns the URLs of names.
func (m *Module) queueURLs(c *call, names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, m.queueURL(c, n))
	}
	return out
}

// entryError renders an entry's error as the batch reports it.
func entryError(c *call, id string, err error) sqsapi.BatchResultErrorEntry {
	apiErr := sqsapi.ToError(c, err)
	return sqsapi.BatchResultErrorEntry{
		ID: id, Code: apiErr.Code, Message: apiErr.Message, SenderFault: apiErr.Sender(),
	}
}
