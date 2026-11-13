// Package inspect serves the inspection API, which is not AWS's: JSON
// routes under /_queues/ to look at every queue and its messages without
// receiving them, and to reset the store between tests.
package inspect

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gasmod/gas"

	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/httpx"
	"tule.dev/queues/internal/modules/queues"
	"tule.dev/queues/internal/sqsapi"
)

const moduleName = "tule/modules/inspect"

// Prefix starts every inspection path.
const Prefix = "/_queues/"

// The page sizes of the message listing.
const (
	defaultLimit = 20
	maxLimit     = 100
)

// errorTypeHeader carries an inspection error's code, as the AWS JSON
// services carry theirs.
const errorTypeHeader = "X-Amzn-Errortype"

// timeLayout is how the API writes a time: RFC 3339 in UTC, to the
// millisecond.
const timeLayout = "2006-01-02T15:04:05.000Z"

// Module registers the inspection routes and serves them.
type Module struct {
	router   *gas.Router
	store    contracts.QueueProvider
	settings *contracts.QueueSettings
	// principals shows the queue policies' principals, nil when the
	// service runs standalone.
	principals contracts.PolicyPrincipals
}

// NewModule returns the constructor gas uses to build a Module over store,
// for the queues of settings' account, showing the queue policies'
// principals with principals, nil to show them as stored (standalone).
func NewModule(settings *contracts.QueueSettings, principals contracts.PolicyPrincipals) func(
	router *gas.Router, store contracts.QueueProvider,
) *Module {
	return func(router *gas.Router, store contracts.QueueProvider) *Module {
		return &Module{router: router, store: store, settings: settings, principals: principals}
	}
}

// Name identifies the module to gas.
func (m *Module) Name() string {
	return moduleName
}

// Init registers the module's routes.
func (m *Module) Init(context.Context) error {
	m.router.Handle(m, http.MethodGet, Prefix+"queues", m.listQueues)
	m.router.Handle(m, http.MethodGet, Prefix+"queues/{name}/messages", m.listMessages)
	m.router.Handle(m, http.MethodDelete, Prefix+"messages", m.purgeAll)
	m.router.Handle(m, http.MethodDelete, Prefix+"queues", m.deleteAll)
	return nil
}

// Close releases nothing; the module holds no resources of its own.
func (m *Module) Close(context.Context) error {
	return nil
}

// Queue is a queue as the API reports it.
type Queue struct {
	Attributes map[string]string `json:"attributes"`
	Name       string            `json:"name"`
	URL        string            `json:"url"`
	ARN        string            `json:"arn"`
	Counts     Counts            `json:"counts"`
}

// Counts are a queue's messages in each state.
type Counts struct {
	Visible  int `json:"visible"`
	InFlight int `json:"inFlight"`
	Delayed  int `json:"delayed"`
}

// Attribute is a message attribute. encoding/json writes BinaryValue as
// standard base64.
type Attribute struct {
	StringValue *string `json:"stringValue,omitempty"`
	DataType    string  `json:"dataType"`
	BinaryValue []byte  `json:"binaryValue,omitempty"`
}

// Message is a message as the API reports it.
type Message struct {
	Attributes        map[string]string    `json:"attributes"`
	MessageAttributes map[string]Attribute `json:"messageAttributes"`
	ID                string               `json:"id"`
	Body              string               `json:"body"`
	SentTime          string               `json:"sentTime"`
	State             string               `json:"state"`
	VisibleAt         string               `json:"visibleAt"`
	Group             string               `json:"group,omitempty"`
	DeduplicationID   string               `json:"deduplicationId,omitempty"`
	SequenceNumber    string               `json:"sequenceNumber,omitempty"`
	ReceiveCount      int                  `json:"receiveCount"`
}

// Page is a page of a listing.
type Page[T any] struct {
	NextCursor string `json:"nextCursor,omitempty"`
	Items      []T    `json:"items"`
}

func (m *Module) listQueues(ctx gas.Context) error {
	qs, err := m.store.ListAllQueues(ctx)
	if err != nil {
		return writeError(ctx, err)
	}
	out := Page[Queue]{Items: make([]Queue, 0, len(qs))}
	for _, q := range qs {
		arn := m.settings.ARN(q.Name)
		attrs := sqsapi.AllAttributes(q, arn)
		// The policy is shown as GetQueueAttributes shows it.
		if err = queues.ShowPolicy(ctx, m.principals, attrs); err != nil {
			return writeError(ctx, err)
		}
		out.Items = append(out.Items, Queue{
			Name:       q.Name,
			URL:        queues.QueueURL(m.settings, ctx.Request(), q.Name),
			ARN:        arn,
			Attributes: attrs,
			Counts: Counts{
				Visible: q.Counts.Visible, InFlight: q.Counts.InFlight, Delayed: q.Counts.Delayed,
			},
		})
	}
	return writeJSON(ctx, out)
}

func (m *Module) listMessages(ctx gas.Context) error {
	f := contracts.MessageFilter{State: ctx.Query("state"), Group: ctx.Query("group")}
	switch f.State {
	case "", contracts.StateDelayed, contracts.StateVisible, contracts.StateInFlight:
	default:
		return writeError(ctx, sqsapi.ErrInvalidParameterValue(
			"state must be delayed, visible or inFlight."))
	}
	limit := defaultLimit
	if v := ctx.Request().URL.Query(); v.Has("limit") {
		n, err := strconv.Atoi(v.Get("limit"))
		if err != nil || n < 1 || n > maxLimit {
			return writeError(ctx, sqsapi.ErrInvalidParameterValue(
				"limit must be an integer from 1 to 100."))
		}
		limit = n
	}
	page, err := m.store.ListMessages(ctx, ctx.Param("name"), f,
		contracts.PageInput{NextToken: ctx.Query("cursor"), MaxResults: limit})
	if err != nil {
		if errors.Is(err, contracts.ErrInvalidToken) {
			return writeError(ctx, sqsapi.ErrInvalidParameterValue("cursor is not valid."))
		}
		return writeError(ctx, err)
	}
	out := Page[Message]{NextCursor: page.NextToken, Items: make([]Message, 0, len(page.Items))}
	for _, msg := range page.Items {
		out.Items = append(out.Items, wireMessage(msg))
	}
	return writeJSON(ctx, out)
}

func wireMessage(msg *contracts.Message) Message {
	attrs := make(map[string]Attribute, len(msg.Attributes))
	for name, a := range msg.Attributes {
		attrs[name] = Attribute{
			DataType: a.DataType, StringValue: a.StringValue,
			BinaryValue: a.BinaryValue,
		}
	}
	return Message{
		ID:                msg.ID,
		Body:              msg.Body,
		Attributes:        sqsapi.SystemAttributes(msg),
		MessageAttributes: attrs,
		SentTime:          formatTime(msg.SentAt),
		State:             msg.State,
		VisibleAt:         formatTime(msg.VisibleAt),
		ReceiveCount:      msg.ReceiveCount,
		Group:             msg.GroupID,
		DeduplicationID:   msg.DeduplicationID,
		SequenceNumber:    msg.SequenceNumber,
	}
}

func formatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

func (m *Module) purgeAll(ctx gas.Context) error {
	if err := m.store.PurgeAll(ctx); err != nil {
		return writeError(ctx, err)
	}
	return ctx.NoContent() //nolint:wrapcheck // the response writer's error
}

func (m *Module) deleteAll(ctx gas.Context) error {
	if err := m.store.DeleteAll(ctx); err != nil {
		return writeError(ctx, err)
	}
	return ctx.NoContent() //nolint:wrapcheck // the response writer's error
}

func writeJSON(ctx gas.Context, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return writeError(ctx, err)
	}
	w := ctx.ResponseWriter()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, wErr := w.Write(body); wErr != nil {
		httpx.Logger(ctx).Warn("failed to write response").Err("error", wErr).Send()
	}
	return nil
}

func writeError(ctx gas.Context, err error) error {
	WriteError(ctx.ResponseWriter(), ctx.Request(), err)
	return nil
}

// errorBody is an inspection error.
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WriteError renders err as an inspection error: its SQS code and message
// as JSON, with the code in X-Amzn-ErrorType too. An unknown queue is 404.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	apiErr := sqsapi.ToError(r.Context(), err)
	status := apiErr.Status
	if apiErr.Code == sqsapi.ErrQueueDoesNotExist().Code {
		status = http.StatusNotFound
	}
	code := apiErr.Code
	if apiErr.Shape != "" {
		code = apiErr.Shape
	}
	body, mErr := json.Marshal(errorBody{Code: code, Message: apiErr.Message})
	if mErr != nil {
		httpx.Logger(r.Context()).Error("failed to encode error response").
			Err("error", mErr).Send()
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(errorTypeHeader, code)
	w.WriteHeader(status)
	if _, wErr := w.Write(body); wErr != nil {
		httpx.Logger(r.Context()).Warn("failed to write error response").
			Err("error", wErr).Send()
	}
}
