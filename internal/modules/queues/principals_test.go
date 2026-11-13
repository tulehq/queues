package queues

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gasmod/gas"

	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/sqsapi"
)

// recordingPrincipals records the documents Bind is asked about.
type recordingPrincipals struct {
	bound []string
}

func (p *recordingPrincipals) Bind(_ context.Context, doc string) (string, error) {
	p.bound = append(p.bound, doc)
	return doc, nil
}

func (p *recordingPrincipals) Show(_ context.Context, doc string) (string, error) {
	return doc, nil
}

// permissionStore runs the bind AddPermission is given on a statement, as
// the store does.
type permissionStore struct {
	contracts.QueueProvider
}

func (permissionStore) AddPermission(
	ctx context.Context, _ string, _ contracts.Permission, bind contracts.BindFunc,
) error {
	if bind == nil {
		return nil
	}
	_, err := bind(ctx, `{"Version":"2012-10-17","Statement":[{"Sid":"a","Effect":"Allow",`+
		`"Principal":{"AWS":["arn:aws:iam::111122223333:root"]},"Action":["SQS:SendMessage"],`+
		`"Resource":"arn:aws:sqs:us-east-1:000000000000:orders"}]}`)
	return err //nolint:wrapcheck // the bind's error, as the store returns it
}

func TestAddPermissionHandsTheStoreTheBinder(t *testing.T) {
	settings := &contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"}
	pp := &recordingPrincipals{}
	m := NewModule(settings, nil, pp)(nil, permissionStore{})
	body := `{"QueueUrl":"http://queues.local/000000000000/orders","Label":"a",` +
		`"AWSAccountIds":["111122223333"],"Actions":["SendMessage"]}`
	r := httptest.NewRequest(http.MethodPost, "http://queues.local/", strings.NewReader(body))
	c := &call{
		Context: gas.NewContext(r.Context(), httptest.NewRecorder(), r),
		op:      "AddPermission", body: []byte(body), proto: sqsapi.JSON,
	}
	if _, err := m.addPermission(c); err != nil {
		t.Fatalf("AddPermission: %v", err)
	}
	if len(pp.bound) != 1 {
		t.Fatalf("Bind asked %d times, want once", len(pp.bound))
	}
}
