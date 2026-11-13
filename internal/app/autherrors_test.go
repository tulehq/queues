package app

import (
	"errors"
	"net/http"
	"testing"

	"tule.dev/sigv4"

	"tule.dev/queues/internal/sqsapi"
)

func TestSigv4ErrorMapsEveryKindTheVerifierReturns(t *testing.T) {
	const text = "the verifier's text"
	tests := []struct {
		code    string
		message string
		kind    sigv4.Kind
		status  int
	}{
		{
			sqsapi.CodeMissingAuthToken, "Request is missing Authentication Token",
			sigv4.KindMissingAuth, http.StatusForbidden,
		},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindUnsupportedAlgorithm, 400},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindIncompleteAuth, 400},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindInvalidDate, 400},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindUnsignedHeader, 400},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindMalformedCredential, 400},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindCredentialDateMismatch, 400},
		{sqsapi.CodeIncompleteSignature, text, sigv4.KindPayloadHashNotAccepted, 400},
		{sqsapi.CodeSignatureDoesNotMatch, text, sigv4.KindRequestTimeTooSkewed, 403},
		{sqsapi.CodeSignatureDoesNotMatch, text, sigv4.KindSignatureMismatch, 403},
		{sqsapi.CodeSignatureDoesNotMatch, text, sigv4.KindWrongService, 403},
		{
			sqsapi.CodeInvalidClientTokenID,
			"The security token included in the request is invalid.",
			sigv4.KindUnknownAccessKey,
			403,
		},
		{
			sqsapi.CodeInvalidClientTokenID,
			"The security token included in the request is invalid.",
			sigv4.KindInvalidSessionToken,
			403,
		},
		{
			sqsapi.CodeExpiredToken, "The security token included in the request is expired",
			sigv4.KindExpiredSessionToken, 403,
		},
		{sqsapi.CodeInvalidParameterValue, text, sigv4.KindPayloadHashMismatch, 400},
		{sqsapi.CodeInvalidParameterValue, text, sigv4.KindBodyUnreadable, 400},
		{sqsapi.CodeInvalidParameterValue, text, sigv4.KindBodyTooLarge, 413},
	}
	for _, tt := range tests {
		t.Run(tt.kind.String(), func(t *testing.T) {
			err := sigv4Error(&sigv4.Error{Kind: tt.kind, Message: text})
			var apiErr *sqsapi.Error
			if !errors.As(err, &apiErr) {
				t.Fatalf("error = %v, want an *sqsapi.Error", err)
			}
			want := sqsapi.Error{Status: tt.status, Code: tt.code, Message: tt.message}
			if *apiErr != want {
				t.Fatalf("error = %+v, want %+v", *apiErr, want)
			}
		})
	}
}

func TestSigv4ErrorLeavesTheRestToInternalFailure(t *testing.T) {
	for _, kind := range []sigv4.Kind{
		sigv4.KindKeyLookupFailed,
		sigv4.KindInvalidExpires,
		sigv4.KindExpired,
		sigv4.KindRequestNotYetValid,
		sigv4.KindMissingPayloadHash,
		sigv4.KindUnverifiedPayload,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			in := &sigv4.Error{Kind: kind, Message: "the verifier's text"}
			var out *sigv4.Error
			if err := sigv4Error(in); !errors.As(err, &out) || out != in {
				t.Fatalf("error = %v, want the *sigv4.Error itself", err)
			}
			got := sqsapi.ToError(t.Context(), in)
			if got.Status != http.StatusInternalServerError ||
				got.Code != sqsapi.CodeInternalFailure {
				t.Fatalf("answered as %d %s, want 500 InternalFailure", got.Status, got.Code)
			}
		})
	}
}
