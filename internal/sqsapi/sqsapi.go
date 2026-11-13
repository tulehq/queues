// Package sqsapi is the SQS wire layer: the two protocols SQS serves on one
// endpoint, the request and response shapes, the input validation the
// service applies before a request reaches the store, the MD5 digests SQS
// computes, and the error documents.
//
// AWS JSON 1.0 is a POST whose X-Amz-Target names the operation under
// AmazonSQS, with a JSON body; it is what current SDKs speak. Query is a
// form-encoded POST naming the operation in Action and the API version in
// Version, sent to the queue URL or to /, and answered in XML; older SDKs
// and libraries speak it. A request with an X-Amz-Target header is a JSON
// request; any other is a Query request.
package sqsapi

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"

	"tule.dev/queues/internal/httpx"
)

// Protocol is the wire protocol of a request.
type Protocol int

// The two protocols.
const (
	JSON Protocol = iota + 1
	Query
)

// The headers and media types of the protocols.
const (
	TargetHeader     = "X-Amz-Target"
	JSONContentType  = "application/x-amz-json-1.0"
	QueryContentType = "text/xml"
	// queryErrorHeader carries the Query error code and fault of a JSON
	// error, as SQS's awsQueryCompatible JSON protocol sends it.
	queryErrorHeader = "X-Amzn-Query-Error"
	requestIDHeader  = "X-Amzn-Requestid"
	targetPrefix     = "AmazonSQS."
	// APIVersion is the Version every Query request names.
	APIVersion = "2012-11-05"
	// Namespace is the XML namespace of every Query response document, as
	// the SQS API reference's examples give it.
	Namespace = "http://queue.amazonaws.com/doc/2012-11-05/"
	// shapeNamespace prefixes an SQS error shape in a JSON error's __type.
	shapeNamespace = "com.amazonaws.sqs#"
)

// ProtocolOf returns the protocol of r.
func ProtocolOf(r *http.Request) Protocol {
	if _, ok := r.Header[http.CanonicalHeaderKey(TargetHeader)]; ok {
		return JSON
	}
	return Query
}

// Operation returns the operation an X-Amz-Target header names, or "" when
// the header is not exactly the target prefix followed by a name.
func Operation(header string) string {
	op, ok := strings.CutPrefix(header, targetPrefix)
	if !ok {
		return ""
	}
	return op
}

// Write sends result as the successful response to op in protocol p. A nil
// result is an operation that returns nothing: an empty body in JSON, as
// the SQS developer guide describes it, and a document with no Result
// element in Query.
func Write(w http.ResponseWriter, r *http.Request, p Protocol, op string, result any) {
	var (
		body []byte
		err  error
	)
	contentType := JSONContentType
	switch {
	case p == Query:
		contentType = QueryContentType
		body, err = queryDocument(r, op, result)
	case result != nil:
		body, err = json.Marshal(result)
	}
	if err != nil {
		WriteError(w, r, p, err)
		return
	}

	w.Header().Set("Content-Type", contentType)
	setRequestID(w, r)
	w.WriteHeader(http.StatusOK)
	//nolint:gosec // an XML or JSON document under its own media type, not HTML
	if _, wErr := w.Write(body); wErr != nil {
		httpx.Logger(r.Context()).Warn("failed to write response").Err("error", wErr).Send()
	}
}

// responseMetadata closes every successful Query response document.
type responseMetadata struct {
	RequestID string `xml:"RequestId"`
}

func queryDocument(r *http.Request, op string, result any) ([]byte, error) {
	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	root := xml.StartElement{
		Name: xml.Name{Local: op + "Response"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: Namespace}},
	}
	err := enc.EncodeToken(root)
	if err == nil && result != nil {
		err = enc.EncodeElement(result, xml.StartElement{Name: xml.Name{Local: op + "Result"}})
	}
	if err == nil {
		err = enc.EncodeElement(responseMetadata{RequestID: httpx.RequestIDOf(r.Context())},
			xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}})
	}
	if err == nil {
		err = enc.EncodeToken(root.End())
	}
	if err == nil {
		err = enc.Flush()
	}
	return buf.Bytes(), err //nolint:wrapcheck // the encoder's error, logged by WriteError
}

// jsonError is the JSON protocol's error document. SQS sends the message
// under "message", as the developer guide's example shows it.
type jsonError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// queryError is the Query protocol's error document, which the developer
// guide shows without a namespace.
type queryError struct {
	XMLName   xml.Name         `xml:"ErrorResponse"`
	Error     queryErrorDetail `xml:"Error"`
	RequestID string           `xml:"RequestId"`
}

type queryErrorDetail struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// fault is the Sender or Receiver the Query error Type and the
// x-amzn-query-error header name.
func fault(e *Error) string {
	if e.Sender() {
		return "Sender"
	}
	return "Receiver"
}

// WriteError renders err as an error response in protocol p.
//
// A JSON error's __type is the SQS shape, namespaced as SQS sends it, or the
// bare code of an error the model has no shape for; the x-amzn-query-error
// header carries the Query code and fault, which the SDKs report as the
// error code.
func WriteError(w http.ResponseWriter, r *http.Request, p Protocol, err error) {
	apiErr := ToError(r.Context(), err)

	var (
		body        []byte
		mErr        error
		contentType string
	)
	if p == Query {
		contentType = QueryContentType
		body, mErr = xml.Marshal(queryError{
			Error: queryErrorDetail{
				Type: fault(apiErr), Code: apiErr.Code, Message: apiErr.Message,
			},
			RequestID: httpx.RequestIDOf(r.Context()),
		})
	} else {
		contentType = JSONContentType
		typ := apiErr.Code
		if apiErr.Shape != "" {
			typ = shapeNamespace + apiErr.Shape
		}
		body, mErr = json.Marshal(jsonError{Type: typ, Message: apiErr.Message})
		w.Header().Set(queryErrorHeader, apiErr.Code+";"+fault(apiErr))
	}
	if mErr != nil {
		httpx.Logger(r.Context()).Error("failed to encode error response").
			Err("error", mErr).Send()
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", contentType)
	setRequestID(w, r)
	w.WriteHeader(apiErr.Status)
	if _, wErr := w.Write(body); wErr != nil {
		httpx.Logger(r.Context()).Warn("failed to write error response").
			Err("error", wErr).Send()
	}
}

func setRequestID(w http.ResponseWriter, r *http.Request) {
	if id := httpx.RequestIDOf(r.Context()); id != "" {
		w.Header().Set(requestIDHeader, id)
	}
}
