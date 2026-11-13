//go:build e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The Query protocol with raw signed form POSTs, since the Go SDK no
// longer speaks it. The XML shapes are those of the API reference's "Using
// AWS query protocol" examples (CreateQueue, SendMessage, ReceiveMessage,
// DeleteMessage) and of the Developer Guide's "Interpreting Amazon SQS XML
// API responses". The namespace differs between those pages, so only the
// element names are asserted.

// queryResponse is an answer to a Query request.
type queryResponse struct {
	Header http.Header
	Body   []byte
	Status int
}

// query sends a form POST of params, with Version 2012-11-05, to path on
// srv, signed SigV4 for sqs with test/test.
func query(t *testing.T, srv *server, path string, params url.Values) queryResponse {
	t.Helper()

	params.Set("Version", "2012-11-05")
	body := params.Encode()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+path, strings.NewReader(body))
	requireNoErr(t, err, "build query request")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	sum := sha256.Sum256([]byte(body))
	err = v4.NewSigner().SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"},
		req, hex.EncodeToString(sum[:]), "sqs", region, time.Now())
	requireNoErr(t, err, "sign query request")

	res, err := srv.Client().Do(req)
	requireNoErr(t, err, "query "+params.Get("Action"))
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	requireNoErr(t, err, "read query response")
	return queryResponse{Status: res.StatusCode, Header: res.Header, Body: b}
}

// decodeXML decodes res into v, failing unless the answer is 200 XML whose
// root element is root.
func decodeXML(t *testing.T, res queryResponse, root string, v any) {
	t.Helper()
	if res.Status != http.StatusOK {
		t.Fatalf("%s: status %d, want 200: %s", root, res.Status, res.Body)
	}
	requireXMLRoot(t, res.Body, root)
	if err := xml.Unmarshal(res.Body, v); err != nil {
		t.Fatalf("decode %s: %v: %s", root, err, res.Body)
	}
}

func requireXMLRoot(t *testing.T, body []byte, root string) {
	t.Helper()
	d := xml.NewDecoder(strings.NewReader(string(body)))
	for {
		tok, err := d.Token()
		if err != nil {
			t.Fatalf("no XML root element in %s: %v", body, err)
		}
		if se, ok := tok.(xml.StartElement); ok {
			requireEqual(t, se.Name.Local, root, "XML root element")
			return
		}
	}
}

// responseMetadata is ResponseMetadata, which every successful answer has.
type responseMetadata struct {
	RequestID string `xml:"ResponseMetadata>RequestId"`
}

func (m responseMetadata) require(t *testing.T, what string) {
	t.Helper()
	if m.RequestID == "" {
		t.Fatalf("%s: no ResponseMetadata/RequestId", what)
	}
}

type createQueueXML struct {
	responseMetadata
	QueueURL string `xml:"CreateQueueResult>QueueUrl"`
}

type sendMessageXML struct {
	responseMetadata
	MessageID              string `xml:"SendMessageResult>MessageId"`
	MD5OfMessageBody       string `xml:"SendMessageResult>MD5OfMessageBody"`
	MD5OfMessageAttributes string `xml:"SendMessageResult>MD5OfMessageAttributes"`
}

type receiveMessageXML struct {
	responseMetadata
	Messages []receivedXML `xml:"ReceiveMessageResult>Message"`
}

// receivedXML is a Message of ReceiveMessageResult.
type receivedXML struct {
	MessageID     string `xml:"MessageId"`
	ReceiptHandle string `xml:"ReceiptHandle"`
	MD5OfBody     string `xml:"MD5OfBody"`
	Body          string `xml:"Body"`
	Attributes    []struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	} `xml:"Attribute"`
}

type deleteMessageXML struct {
	responseMetadata
}

// errorXML is ErrorResponse: Error with Type, Code and Message, and
// RequestId.
type errorXML struct {
	Type      string `xml:"Error>Type"`
	Code      string `xml:"Error>Code"`
	Message   string `xml:"Error>Message"`
	RequestID string `xml:"RequestId"`
}

func TestQueryProtocolRoundTrip(t *testing.T) {
	c, srv := newClient(t)

	// CreateQueue to /, as the API reference example sends it.
	var created createQueueXML
	decodeXML(t, query(t, srv, "/", url.Values{
		"Action":            {"CreateQueue"},
		"QueueName":         {"MyQueue"},
		"Attribute.1.Name":  {"VisibilityTimeout"},
		"Attribute.1.Value": {"40"},
	}), "CreateQueueResponse", &created)
	created.require(t, "CreateQueue")
	requireEqual(t, created.QueueURL, srv.queueURL("MyQueue"), "QueueUrl")
	requireEqual(t, queueAttrs(t, c, created.QueueURL, "VisibilityTimeout")["VisibilityTimeout"],
		"40", "VisibilityTimeout set through Query")

	path := "/" + defaultAccountID + "/MyQueue"

	// SendMessage to the queue's path, with the example's two attributes and
	// its digests.
	var sent sendMessageXML
	decodeXML(t, query(t, srv, path, url.Values{
		"Action":                               {"SendMessage"},
		"MessageBody":                          {"This is a test message"},
		"MessageAttribute.1.Name":              {"my_attribute_name_1"},
		"MessageAttribute.1.Value.StringValue": {"my_attribute_value_1"},
		"MessageAttribute.1.Value.DataType":    {"String"},
		"MessageAttribute.2.Name":              {"my_attribute_name_2"},
		"MessageAttribute.2.Value.StringValue": {"my_attribute_value_2"},
		"MessageAttribute.2.Value.DataType":    {"String"},
	}), "SendMessageResponse", &sent)
	sent.require(t, "SendMessage")
	if sent.MessageID == "" {
		t.Fatal("SendMessage: no MessageId")
	}
	requireEqual(t, sent.MD5OfMessageBody, "fafb00f5732ab283681e124bf8747ed1", "MD5OfMessageBody")
	requireEqual(t, sent.MD5OfMessageAttributes, "c48838208d2b4e14e3ca0093a8443f09",
		"MD5OfMessageAttributes")

	m := queryReceive(t, srv, path, sent.MessageID)

	// DeleteMessage.
	var deleted deleteMessageXML
	decodeXML(t, query(t, srv, path, url.Values{
		"Action": {"DeleteMessage"}, "ReceiptHandle": {m.ReceiptHandle},
	}), "DeleteMessageResponse", &deleted)
	deleted.require(t, "DeleteMessage")
	requireCounts(t, c, created.QueueURL, counts{}, "after the Query delete")
}

// queryReceive receives the one message with ID id at path through Query
// ReceiveMessage with AttributeName=All, checking its XML shape.
func queryReceive(t *testing.T, srv *server, path, id string) receivedXML {
	t.Helper()
	var got receiveMessageXML
	decodeXML(t, query(t, srv, path, url.Values{
		"Action":              {"ReceiveMessage"},
		"MaxNumberOfMessages": {"5"},
		"VisibilityTimeout":   {"15"},
		"AttributeName":       {"All"},
	}), "ReceiveMessageResponse", &got)
	got.require(t, "ReceiveMessage")
	requireEqual(t, len(got.Messages), 1, "received messages")
	m := got.Messages[0]
	requireEqual(t, m.MessageID, id, "MessageId")
	requireEqual(t, m.Body, "This is a test message", "Body")
	requireEqual(t, m.MD5OfBody, "fafb00f5732ab283681e124bf8747ed1", "MD5OfBody")
	if m.ReceiptHandle == "" {
		t.Fatal("ReceiveMessage: no ReceiptHandle")
	}
	attrs := map[string]string{}
	for _, a := range m.Attributes {
		attrs[a.Name] = a.Value
	}
	for _, k := range []string{"SenderId", "ApproximateFirstReceiveTimestamp", "ApproximateReceiveCount", "SentTimestamp"} {
		if attrs[k] == "" {
			t.Fatalf("ReceiveMessage: Attribute %s missing: %v", k, attrs)
		}
	}
	requireEqual(t, attrs["ApproximateReceiveCount"], "1", "ApproximateReceiveCount")

	return m
}

func TestQueryProtocolSendsMessageAttributesOfEveryType(t *testing.T) {
	// The Query form of a Binary attribute is base64 in BinaryValue.
	c, srv := newClient(t)
	u := createQueue(t, c, "typed", nil)
	var sent sendMessageXML
	decodeXML(t, query(t, srv, "/"+defaultAccountID+"/typed", url.Values{
		"Action":                               {"SendMessage"},
		"MessageBody":                          {"typed"},
		"MessageAttribute.1.Name":              {"n"},
		"MessageAttribute.1.Value.StringValue": {"42"},
		"MessageAttribute.1.Value.DataType":    {"Number"},
		"MessageAttribute.2.Name":              {"b"},
		"MessageAttribute.2.Value.BinaryValue": {"AAH+/w=="},
		"MessageAttribute.2.Value.DataType":    {"Binary.raw"},
	}), "SendMessageResponse", &sent)

	want := map[string]types.MessageAttributeValue{
		"n": stringAttr("Number", "42"),
		"b": binaryAttr("Binary.raw", []byte{0x00, 0x01, 0xfe, 0xff}),
	}
	requireEqual(t, sent.MD5OfMessageAttributes, attrsMD5(want), "MD5OfMessageAttributes")
	requireAttrs(t, receiveOne(t, c, u).MessageAttributes, want)
}

func TestQueryProtocolErrors(t *testing.T) {
	c, srv := newClient(t)
	createQueue(t, c, "exists", nil)

	for _, tt := range []struct {
		name   string
		path   string
		params url.Values
		code   string
		status int
	}{
		{
			name:   "a missing queue",
			path:   "/" + defaultAccountID + "/missing",
			params: url.Values{"Action": {"ReceiveMessage"}},
			code:   "AWS.SimpleQueueService.NonExistentQueue",
			status: http.StatusBadRequest,
		},
		{
			name:   "an invalid queue name",
			path:   "/",
			params: url.Values{"Action": {"CreateQueue"}, "QueueName": {"quename_nonalpha!"}},
			code:   "InvalidParameterValue",
			status: http.StatusBadRequest,
		},
		{
			name: "an invalid receipt handle",
			path: "/" + defaultAccountID + "/exists",
			params: url.Values{
				"Action": {"DeleteMessage"}, "ReceiptHandle": {"bogus"},
			},
			code: "ReceiptHandleIsInvalid",
			// The SDK schema's httpError for ReceiptHandleIsInvalid.
			status: http.StatusNotFound,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := query(t, srv, tt.path, tt.params)
			requireEqual(t, res.Status, tt.status, "status: "+string(res.Body))
			requireXMLRoot(t, res.Body, "ErrorResponse")
			var e errorXML
			if err := xml.Unmarshal(res.Body, &e); err != nil {
				t.Fatalf("decode ErrorResponse: %v: %s", err, res.Body)
			}
			requireEqual(t, e.Code, tt.code, "Error/Code")
			requireEqual(t, e.Type, "Sender", "Error/Type")
			if e.Message == "" || e.RequestID == "" {
				t.Fatalf("ErrorResponse lacks a Message or a RequestId: %s", res.Body)
			}
		})
	}
}
