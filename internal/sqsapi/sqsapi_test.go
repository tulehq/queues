package sqsapi

import (
	"crypto/md5" //nolint:gosec // the digest under test is MD5
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"tule.dev/queues/internal/contracts"
)

func str(s string) *string { return &s }

func stringAttr(v string) contracts.MessageAttribute {
	return contracts.MessageAttribute{DataType: "String", StringValue: str(v)}
}

// The digests the SQS API reference's examples show, which a client checks
// a reply against: the SendMessage example's body and two attributes, and
// the SendMessageBatch examples' single attributes.
func TestDigestsMatchTheAPIReferenceExamples(t *testing.T) {
	if got := MD5OfBody("This is a test message"); got != "fafb00f5732ab283681e124bf8747ed1" {
		t.Fatalf("body digest = %s", got)
	}
	tests := []struct {
		attrs map[string]contracts.MessageAttribute
		want  string
	}{
		{map[string]contracts.MessageAttribute{
			"my_attribute_name_1": stringAttr("my_attribute_value_1"),
			"my_attribute_name_2": stringAttr("my_attribute_value_2"),
		}, "c48838208d2b4e14e3ca0093a8443f09"},
		{map[string]contracts.MessageAttribute{
			"my_attribute_name_1": stringAttr("my_attribute_value_1"),
		}, "8ef4d60dbc8efda9f260e1dfd09d29f3"},
		{map[string]contracts.MessageAttribute{
			"test_attribute_name_1": stringAttr("test_attribute_value_1"),
		}, "ba056227cfd9533dba1f72ad9816d233"},
	}
	for _, tt := range tests {
		if got := MD5OfAttributes(tt.attrs); got != tt.want {
			t.Errorf("attribute digest of %v = %s, want %s", tt.attrs, got, tt.want)
		}
	}
	if got := MD5OfAttributes(nil); got != "" {
		t.Fatalf("digest of no attributes = %q, want none", got)
	}
}

// A Binary attribute travels as transport type 2 with its raw bytes, and a
// custom type's suffix is part of the digest.
func TestBinaryAndCustomTypesDigest(t *testing.T) {
	var want []byte
	put := func(b []byte) {
		want = binary.BigEndian.AppendUint32(want, uint32(len(b))) //nolint:gosec // small
		want = append(want, b...)
	}
	put([]byte("a"))
	put([]byte("Binary.gzip"))
	want = append(want, 2)
	put([]byte{0, 1, 2})
	put([]byte("b"))
	put([]byte("Number.float"))
	want = append(want, 1)
	put([]byte("1.5"))
	sum := md5.Sum(want) //nolint:gosec // see the import

	got := MD5OfAttributes(map[string]contracts.MessageAttribute{
		"b": {DataType: "Number.float", StringValue: str("1.5")},
		"a": {DataType: "Binary.gzip", BinaryValue: []byte{0, 1, 2}},
	})
	if got != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest = %s, want %s", got, hex.EncodeToString(sum[:]))
	}
}

func TestQueryDecodesMessageAttributes(t *testing.T) {
	form := url.Values{
		"QueueUrl":                             {"http://h/000000000000/q"},
		"MessageBody":                          {"hi"},
		"DelaySeconds":                         {"5"},
		"MessageAttribute.1.Name":              {"s"},
		"MessageAttribute.1.Value.DataType":    {"String"},
		"MessageAttribute.1.Value.StringValue": {"v"},
		"MessageAttribute.2.Name":              {"b"},
		"MessageAttribute.2.Value.DataType":    {"Binary"},
		"MessageAttribute.2.Value.BinaryValue": {"AAEC"},
	}
	var req SendMessageRequest
	if err := Params(form).Decode(&req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if *req.MessageBody != "hi" || *req.DelaySeconds != 5 ||
		*req.MessageAttributes["s"].StringValue != "v" ||
		string(req.MessageAttributes["b"].BinaryValue) != "\x00\x01\x02" {
		t.Fatalf("decoded %+v", req)
	}
}

func TestQueryDecodesShorthandAndOrderedMembers(t *testing.T) {
	var attrs SetQueueAttributesRequest
	if err := Params(url.Values{
		"Attribute.Name": {"VisibilityTimeout"}, "Attribute.Value": {"35"},
	}).Decode(&attrs); err != nil || attrs.Attributes["VisibilityTimeout"] != "35" {
		t.Fatalf("shorthand attribute = %+v, %v", attrs, err)
	}

	var get GetQueueAttributesRequest
	if err := Params(url.Values{
		"AttributeName.2": {"DelaySeconds"}, "AttributeName.1": {"VisibilityTimeout"},
	}).Decode(&get); err != nil ||
		!slices.Equal(get.AttributeNames, []string{"VisibilityTimeout", "DelaySeconds"}) {
		t.Fatalf("attribute names = %v, %v", get.AttributeNames, err)
	}

	var batch DeleteMessageBatchRequest
	if err := Params(url.Values{
		"DeleteMessageBatchRequestEntry.2.Id":            {"b"},
		"DeleteMessageBatchRequestEntry.2.ReceiptHandle": {"hb"},
		"DeleteMessageBatchRequestEntry.1.Id":            {"a"},
		"DeleteMessageBatchRequestEntry.1.ReceiptHandle": {"ha"},
	}).Decode(&batch); err != nil || len(batch.Entries) != 2 || *batch.Entries[0].ID != "a" {
		t.Fatalf("batch = %+v, %v", batch, err)
	}

	var bad ReceiveMessageRequest
	var apiErr *Error
	err := Params(url.Values{"MaxNumberOfMessages": {"ten"}}).Decode(&bad)
	if !errors.As(err, &apiErr) || apiErr.Code != CodeInvalidParameterValue {
		t.Fatalf("a non-integer = %v, want InvalidParameterValue", err)
	}
}

func TestQueryWritesTheResultDocument(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	w := httptest.NewRecorder()
	Write(w, r, Query, "ReceiveMessage", ReceiveMessageResult{Messages: []Message{{
		MessageID: "id", ReceiptHandle: "rh", MD5OfBody: "md5", Body: "a<b",
		Attributes: AttributeMap{"SenderId": "AKID"},
		MessageAttributes: MessageAttributeMap{
			"bin": {DataType: "Binary", BinaryValue: Blob{0, 1}},
		},
	}}})

	want := `<ReceiveMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">` +
		`<ReceiveMessageResult><Message>` +
		`<Attribute><Name>SenderId</Name><Value>AKID</Value></Attribute>` +
		`<MessageAttribute><Name>bin</Name><Value><DataType>Binary</DataType>` +
		`<BinaryValue>AAE=</BinaryValue></Value></MessageAttribute>` +
		`<MessageId>id</MessageId><ReceiptHandle>rh</ReceiptHandle><MD5OfBody>md5</MD5OfBody>` +
		`<Body>a&lt;b</Body></Message></ReceiveMessageResult>` +
		`<ResponseMetadata><RequestId></RequestId></ResponseMetadata></ReceiveMessageResponse>`
	if got := w.Body.String(); got != want {
		t.Fatalf("document =\n%s\nwant\n%s", got, want)
	}
	if ct := w.Header().Get("Content-Type"); ct != QueryContentType {
		t.Fatalf("content type = %s", ct)
	}
}

func TestQueryWritesTheErrorDocument(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	w := httptest.NewRecorder()
	WriteError(w, r, Query, ErrQueueDoesNotExist())

	want := `<ErrorResponse><Error><Type>Sender</Type>` +
		`<Code>AWS.SimpleQueueService.NonExistentQueue</Code>` +
		`<Message>The specified queue does not exist.</Message></Error>` +
		`<RequestId></RequestId></ErrorResponse>`
	if w.Code != http.StatusBadRequest || w.Body.String() != want {
		t.Fatalf("error = %d %s", w.Code, w.Body.String())
	}
}

func TestJSONErrorCarriesTheShapeAndTheQueryCode(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	w := httptest.NewRecorder()
	WriteError(w, r, JSON, ErrQueueDoesNotExist())

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", w.Body.String(), err)
	}
	if body["__type"] != "com.amazonaws.sqs#QueueDoesNotExist" ||
		body["message"] != "The specified queue does not exist." {
		t.Fatalf("body = %v", body)
	}
	if got := w.Header().Get("X-Amzn-Query-Error"); got !=
		"AWS.SimpleQueueService.NonExistentQueue;Sender" {
		t.Fatalf("x-amzn-query-error = %q", got)
	}
}

// TestErrorShapesAreTheSDKs holds every error shape this service sends to
// the SDK's, by name and by the Query code its awsQueryError trait gives, so
// a misspelling that would reach clients as an unknown error fails here.
func TestErrorShapesAreTheSDKs(t *testing.T) {
	modeled := []smithy.APIError{
		&types.QueueDoesNotExist{}, &types.QueueNameExists{}, &types.QueueDeletedRecently{},
		&types.PurgeQueueInProgress{}, &types.ReceiptHandleIsInvalid{},
		&types.MessageNotInflight{}, &types.InvalidAttributeName{},
		&types.InvalidAttributeValue{}, &types.InvalidMessageContents{},
		&types.InvalidAddress{}, &types.OverLimit{}, &types.EmptyBatchRequest{},
		&types.TooManyEntriesInBatchRequest{}, &types.BatchEntryIdsNotDistinct{},
		&types.InvalidBatchEntryId{}, &types.BatchRequestTooLong{}, &types.UnsupportedOperation{},
	}
	names := map[string]bool{}
	for _, e := range modeled {
		names[e.ErrorCode()] = true
	}
	shapes := []errorShape{
		shapeQueueDoesNotExist, shapeQueueNameExists, shapeQueueDeletedRecently,
		shapePurgeQueueInProgress, shapeReceiptHandleIsInvalid, shapeMessageNotInflight,
		shapeInvalidAttributeName, shapeInvalidAttributeValue, shapeInvalidMessageContents,
		shapeInvalidAddress, shapeOverLimit, shapeEmptyBatch, shapeTooManyEntries,
		shapeIDsNotDistinct, shapeInvalidBatchEntryID, shapeBatchTooLong,
		shapeUnsupportedOperation,
	}
	for _, s := range shapes {
		if !names[s.name] {
			t.Errorf("%s is not an SQS error shape the SDK models", s.name)
		}
	}
}

// sendRuleCase is a send CheckSend refuses with code.
type sendRuleCase struct {
	f    SendFields
	name string
	code string
	q    SendTarget
}

func rule(name, code string, f SendFields, q SendTarget) sendRuleCase {
	return sendRuleCase{name: name, code: code, f: f, q: q}
}

var (
	stdTarget  = SendTarget{MaximumMessageSize: MaxMessageSize}
	fifoTarget = SendTarget{MaximumMessageSize: MaxMessageSize, FIFO: true}
)

func sendRuleCases() []sendRuleCase {
	std, fifo := stdTarget, fifoTarget
	return []sendRuleCase{
		rule("no body", CodeMissingParameter, SendFields{}, std),
		rule("a control character", "InvalidMessageContents", SendFields{Body: str("a\x01")}, std),
		rule(
			"a delay of 901", CodeInvalidParameterValue,
			SendFields{Body: str("a"), DelaySeconds: new(901)},
			std,
		),
		rule(
			"dedup on a standard queue", CodeInvalidParameterValue,
			SendFields{Body: str("a"), DeduplicationID: str("d")},
			std,
		),
		rule("FIFO without a group", CodeMissingParameter, SendFields{Body: str("a")}, fifo),
		rule(
			"FIFO without a dedup ID", CodeInvalidParameterValue,
			SendFields{Body: str("a"), GroupID: str("g")},
			fifo,
		),
		rule(
			"FIFO with a delay", CodeInvalidParameterValue,
			SendFields{
				Body: str("a"), GroupID: str("g"), DelaySeconds: new(5),
				DeduplicationID: str("d"),
			},
			fifo,
		),
		rule(
			"too large", CodeInvalidParameterValue,
			SendFields{Body: str(strings.Repeat("a", 1025))},
			SendTarget{MaximumMessageSize: 1024},
		),
		rule("eleven attributes", CodeInvalidParameterValue, SendFields{
			Body:       str("a"),
			Attributes: elevenAttributes(),
		}, std),
		rule("a reserved attribute name", CodeInvalidParameterValue, SendFields{
			Body: str("a"),
			Attributes: map[string]MessageAttributeValue{
				"AWS.x": {DataType: "String", StringValue: str("v")},
			},
		}, std),
		rule("a number that is not one", CodeInvalidParameterValue, SendFields{
			Body: str("a"),
			Attributes: map[string]MessageAttributeValue{
				"n": {DataType: "Number", StringValue: str("1e")},
			},
		}, std),
	}
}

func TestCheckSendHoldsTheQueueTypeRules(t *testing.T) {
	cbd := SendTarget{
		MaximumMessageSize: MaxMessageSize, FIFO: true, ContentBasedDeduplication: true,
	}
	tests := sendRuleCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := CheckSend(tt.f, tt.q)
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.Code != tt.code {
				t.Fatalf("check = %v, want %s", err, tt.code)
			}
		})
	}

	in, _, err := CheckSend(SendFields{Body: str("a"), GroupID: str("g")}, cbd)
	if err != nil || in.DeduplicationID != contentDeduplicationID("a") {
		t.Fatalf("content-based deduplication = %+v, %v", in, err)
	}
}

func elevenAttributes() map[string]MessageAttributeValue {
	out := map[string]MessageAttributeValue{}
	for i := range 11 {
		out[fmt.Sprint("a", i)] = MessageAttributeValue{DataType: "String", StringValue: str("v")}
	}
	return out
}

func TestCheckNumber(t *testing.T) {
	for _, ok := range []string{"0", "-1.5", "1e126", "1.23e-128", "00012", ".5"} {
		if err := checkNumber("n", ok); err != nil {
			t.Errorf("checkNumber(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "x", "1e127", "1e-129", strings.Repeat("9", 39), "1..2"} {
		if err := checkNumber("n", bad); err == nil {
			t.Errorf("checkNumber(%q) accepted", bad)
		}
	}
}

func TestQueueAttributeChecks(t *testing.T) {
	_, fifo, err := CheckCreateAttributes("q.fifo", map[string]string{"FifoQueue": "true"})
	if err != nil || !fifo {
		t.Fatalf("FIFO create = %v, %v", fifo, err)
	}
	for name, attrs := range map[string]map[string]string{
		"q.fifo": nil,
		"q":      {"FifoQueue": "true"},
	} {
		if _, _, err = CheckCreateAttributes(name, attrs); err == nil {
			t.Errorf("create %s with %v accepted", name, attrs)
		}
	}

	bad := []map[string]string{
		{"VisibilityTimeout": "43201"},
		{"Nonsense": "1"},
		{"ContentBasedDeduplication": "true"},
		{"RedrivePolicy": `{"maxReceiveCount":5}`},
		{"RedrivePolicy": `{"deadLetterTargetArn":"a","maxReceiveCount":1001}`},
		{"RedriveAllowPolicy": `{"redrivePermission":"byQueue"}`},
		{"RedriveAllowPolicy": `{"redrivePermission":"allowAll","sourceQueueArns":["a"]}`},
	}
	for _, attrs := range bad {
		if _, err = CheckQueueAttributes(attrs, contracts.Defaults(false), false); err == nil {
			t.Errorf("attributes %v accepted on a standard queue", attrs)
		}
	}
	out, err := CheckQueueAttributes(map[string]string{
		"RedrivePolicy": `{"maxReceiveCount":"5","deadLetterTargetArn":"arn"}`,
	}, contracts.Defaults(false), false)
	if err != nil || out["RedrivePolicy"] != `{"deadLetterTargetArn":"arn","maxReceiveCount":5}` {
		t.Fatalf("normalized redrive policy = %v, %v", out, err)
	}
	if _, err = CheckQueueAttributes(map[string]string{"FifoThroughputLimit": "perMessageGroupId"},
		contracts.Defaults(true), true); err == nil {
		t.Fatal("perMessageGroupId accepted with DeduplicationScope queue")
	}
}

func TestReceivedPicksTheAttributesAskedFor(t *testing.T) {
	m := &contracts.Message{
		ID: "id", Body: "b", SenderID: "AKID", ReceiveCount: 1,
		Attributes: map[string]contracts.MessageAttribute{
			"bar.one": stringAttr("1"), "bar.two": stringAttr("2"), "other": stringAttr("3"),
		},
	}
	got := Received(m, []string{SysSenderID}, []string{"bar.*"})
	if len(got.Attributes) != 1 || got.Attributes[SysSenderID] != "AKID" {
		t.Fatalf("system attributes = %v", got.Attributes)
	}
	if len(got.MessageAttributes) != 2 || got.MD5OfMessageAttributes != MD5OfAttributes(
		map[string]contracts.MessageAttribute{
			"bar.one": stringAttr("1"), "bar.two": stringAttr("2"),
		}) {
		t.Fatalf("message attributes = %v, digest %s", got.MessageAttributes,
			got.MD5OfMessageAttributes)
	}
	if none := Received(m, nil, nil); none.MessageAttributes != nil || none.Attributes != nil {
		t.Fatalf("a receive asking for nothing got %+v", none)
	}
}

func requireCodeOf(t *testing.T, err error, code, what string) {
	t.Helper()
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		t.Fatalf("%s: error = %v, want %s", what, err, code)
	}
}

func TestARepeatedTagKeyIsRefused(t *testing.T) {
	var ok TagQueueRequest
	if err := json.Unmarshal([]byte(`{"Tags":{"team":"blue","env":"dev"}}`), &ok); err != nil ||
		len(ok.Tags) != 2 || ok.Tags["team"] != "blue" {
		t.Fatalf("tags = %v, %v", ok.Tags, err)
	}
	var none CreateQueueRequest
	if err := json.Unmarshal([]byte(`{"tags":null}`), &none); err != nil || none.Tags != nil {
		t.Fatalf("null tags = %v, %v", none.Tags, err)
	}

	var dup CreateQueueRequest
	requireCodeOf(t, json.Unmarshal([]byte(`{"tags":{"team":"blue","team":"red"}}`), &dup),
		CodeInvalidParameterValue, "a repeated JSON key")
	// A key spelled with an escape is the same key.
	requireCodeOf(t, json.Unmarshal([]byte(`{"tags":{"team":"blue","team":"red"}}`), &dup),
		CodeInvalidParameterValue, "a repeated, escaped JSON key")
	requireCodeOf(t, json.Unmarshal([]byte(`{"tags":{"team":1}}`), &dup),
		CodeSerialization, "a value that is not a string")
	requireCodeOf(t, json.Unmarshal([]byte(`{"tags":["team"]}`), &dup),
		CodeSerialization, "tags that are not an object")

	var query TagQueueRequest
	requireCodeOf(t, Params(url.Values{
		"Tag.1.Key": {"team"}, "Tag.1.Value": {"blue"},
		"Tag.2.Key": {"team"}, "Tag.2.Value": {"red"},
	}).Decode(&query), CodeInvalidParameterValue, "a repeated Query key")
}

func TestCheckFormRefusesFormsReadTwoWays(t *testing.T) {
	accepted := []string{
		"Action=TagQueue&Tag.1.Key=env&Tag.1.Value=dev&Tag.2.Key=team&Tag.2.Value=blue",
		"Action=TagQueue&Tag.Key=env&Tag.Value=dev",
		"Action=GetQueueAttributes&AttributeName=All",
		"Action=ReceiveMessage&AttributeName.1=All&AttributeName.1=All",
		"Action=TagQueue&Tag.1.Key=env&Tag.1.Key=team&Tag.1.Value=dev",
		"Action=GetQueueAttributes&AttributeName.1=All&AttributeName.10=Policy",
		"Action=SendMessage&MessageAttribute.1.Name=a&MessageAttribute.1.Value.DataType=String" +
			"&MessageAttribute.1.Value.StringListValue.1=x&MessageAttribute.1.Value.StringListValue.2=y",
	}
	for _, f := range accepted {
		form, err := url.ParseQuery(f)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		if err = CheckForm(form); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	refused := []string{
		"Tag.1.Key=env&Tag.1.Value=dev&Tag.01.Value=prod",
		"Tag.1.Key=env&Tag.+1.Value=prod",
		"Tag.0.Key=env&Tag.0.Value=dev",
		"Tag.-1.Key=env&Tag.-1.Value=dev",
		"Tag.1.Key=env&Tag.1.Value=dev&Tag.Key=env&Tag.Value=prod",
		"AttributeName=All&AttributeName.1=Policy",
		"TagKey.1=a&TagKey.01=b",
		"SendMessageBatchRequestEntry.1.Id=a&SendMessageBatchRequestEntry.01.Id=b",
	}
	for _, f := range refused {
		form, err := url.ParseQuery(f)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		requireCodeOf(t, CheckForm(form), CodeMalformedQueryString, f)
	}
}

func TestAStoreTagCaseCollisionIsInvalidParameterValue(t *testing.T) {
	e := ToError(t.Context(), &contracts.TagKeyCaseError{Key: "env"})
	if e.Code != CodeInvalidParameterValue || e.Status != http.StatusBadRequest ||
		!strings.Contains(e.Message, "differs only by case") {
		t.Fatalf("error = %+v", e)
	}
}

func TestARepeatedQueryParameterReadsItsFirstValue(t *testing.T) {
	form, err := url.ParseQuery("Tag.1.Key=env&Tag.1.Key=team&Tag.1.Value=dev&Tag.1.Value=prod")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err = CheckForm(form); err != nil {
		t.Fatalf("CheckForm: %v", err)
	}
	for range 20 {
		var req TagQueueRequest
		if err = Params(
			form,
		).Decode(&req); err != nil || len(req.Tags) != 1 ||
			req.Tags["env"] != "dev" {
			t.Fatalf("tags = %v, %v, want env=dev", req.Tags, err)
		}
	}
}
