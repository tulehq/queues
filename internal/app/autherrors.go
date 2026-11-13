package app

import (
	"net/http"

	"tule.dev/sigv4"

	"tule.dev/queues/internal/sqsapi"
)

// sigv4Error maps a verification failure to the SQS error it is answered
// with: the codes of the SQS common errors reference, which the Query
// protocol sends and the JSON protocol carries in x-amzn-query-error, with
// the statuses that reference gives. An unknown key, and a session token
// that is not the key's, are InvalidClientTokenId; an expired token is
// ExpiredToken; a wrong signature, a request dated too far from the clock,
// or one signed for another service, is SignatureDoesNotMatch; a malformed
// signature is IncompleteSignature.
//
// KindKeyLookupFailed, and the Kinds this configuration cannot produce
// (presigned URLs, a streamed body), are returned as the *sigv4.Error
// itself: ToError answers any error it does not know with InternalFailure
// and logs it, so they surface as a 500 with their cause in the log. A
// failed lookup at identity is never answered as an unknown key.
func sigv4Error(e *sigv4.Error) error {
	switch e.Kind {
	case sigv4.KindMissingAuth:
		return sqsapi.NewError(http.StatusForbidden, sqsapi.CodeMissingAuthToken,
			"Request is missing Authentication Token")
	case sigv4.KindUnsupportedAlgorithm, sigv4.KindIncompleteAuth, sigv4.KindInvalidDate,
		sigv4.KindUnsignedHeader, sigv4.KindMalformedCredential,
		sigv4.KindCredentialDateMismatch, sigv4.KindPayloadHashNotAccepted:
		return sqsapi.NewError(http.StatusBadRequest, sqsapi.CodeIncompleteSignature, e.Message)
	case sigv4.KindSignatureMismatch, sigv4.KindRequestTimeTooSkewed, sigv4.KindWrongService:
		return sqsapi.NewError(http.StatusForbidden, sqsapi.CodeSignatureDoesNotMatch, e.Message)
	case sigv4.KindUnknownAccessKey, sigv4.KindInvalidSessionToken:
		return sqsapi.NewError(http.StatusForbidden, sqsapi.CodeInvalidClientTokenID,
			"The security token included in the request is invalid.")
	case sigv4.KindExpiredSessionToken:
		return sqsapi.NewError(http.StatusForbidden, sqsapi.CodeExpiredToken,
			"The security token included in the request is expired")
	case sigv4.KindBodyUnreadable, sigv4.KindPayloadHashMismatch:
		return sqsapi.ErrInvalidParameterValue(e.Message)
	case sigv4.KindBodyTooLarge:
		return sqsapi.NewError(http.StatusRequestEntityTooLarge,
			sqsapi.CodeInvalidParameterValue, e.Message)
	default:
		return e
	}
}
