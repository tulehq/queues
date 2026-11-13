package app

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"tule.dev/identity/keysource"
	"tule.dev/sigv4"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/modules/health"
	"tule.dev/queues/internal/modules/inspect"
	"tule.dev/queues/internal/sqsapi"
)

// maxBodySize bounds the request body the verifier buffers to hash it. SQS
// takes messages of up to 1 MiB, and batches of up to 1 MiB in all, but the
// request carrying them is larger: a JSON string escapes a quote or a
// backslash in two bytes, and a form escapes a four-byte character in
// twelve. Four times the message limit holds the largest batch however its
// characters are encoded.
const maxBodySize = 4 * sqsapi.MaxMessageSize

// identityTimeout bounds a key lookup at identity, so an identity that hangs
// fails the request with InternalFailure rather than holding it until the
// server's own timeout. A lookup is one small local request. It is a
// variable so a test can shorten it.
var identityTimeout = 10 * time.Second

// verifiers are the two SigV4 verifiers, one per protocol: a JSON request
// must sign X-Amz-Target, which names its operation, so a relay cannot turn
// a signed read into a delete with the same body; a Query request names its
// operation in the signed body, and an inspection request in the signed
// method and path.
type verifiers struct {
	json  *sigv4.Verifier
	plain *sigv4.Verifier
}

// newVerifiers returns the SigV4 verifiers every request but the probes goes
// through. They know the configured root key, belonging to the account's
// root user, and, when an identity endpoint is configured, every key
// identity issued, asked of identity on each request after the root key.
// What a key they accept may do is decided after them: the root key may do
// everything, and any other is evaluated against its identity policies.
// Requests are accepted signed for sqs in any region. The path is
// canonicalized as every service but S3 signs it, since Query requests go
// to queue URL paths.
func newVerifiers(cfg *Config) (*verifiers, error) {
	account := cfg.Queues.AccountID
	root, err := sigv4.NewStaticKey(sigv4.StaticKeyConfig{
		AccessKeyID:     cfg.Auth.AccessKeyID,
		SecretAccessKey: cfg.Auth.SecretAccessKey,
		AccountID:       account,
		ARN:             "arn:aws:iam::" + account + ":root",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build the signature verifier: %w", err)
	}
	var keys sigv4.KeySource = root
	if cfg.Auth.IdentityEndpoint != "" {
		identity, kErr := keysource.New(keysource.Config{
			HTTPClient:      &http.Client{Timeout: identityTimeout},
			Endpoint:        cfg.Auth.IdentityEndpoint,
			AccessKeyID:     cfg.Auth.AccessKeyID,
			SecretAccessKey: cfg.Auth.SecretAccessKey,
		})
		if kErr != nil {
			return nil, fmt.Errorf("failed to build the signature verifier: %w", kErr)
		}
		keys = sigv4.Chain(root, identity)
	}
	build := func(required ...string) (*sigv4.Verifier, error) {
		v, vErr := sigv4.New(
			keys,
			sigv4.WithBufferedPayload(maxBodySize),
			sigv4.WithRequiredSignedHeaders(required...),
			sigv4.WithService("sqs"),
			sigv4.WithErrorMap(sigv4Error),
		)
		if vErr != nil {
			return nil, fmt.Errorf("failed to build the signature verifier: %w", vErr)
		}
		return v, nil
	}
	jsonV, err := build("host", "x-amz-date", "x-amz-target")
	if err != nil {
		return nil, err
	}
	plainV, err := build("host", "x-amz-date")
	if err != nil {
		return nil, err
	}
	return &verifiers{json: jsonV, plain: plainV}, nil
}

// verifySignature rejects requests whose SigV4 signature does not verify,
// and serves the rest as Verify returns them, with the signer's principal
// in their context and the body the signature covered. A request carrying
// X-Amz-Target is verified as a JSON request, any other as a Query or
// inspection request.
//
// The health probes are exempt, so a container's health check needs no
// credentials; nothing else is, because any web page the developer has open
// can reach a server on localhost, and this one can delete every queue.
func verifySignature(vs *verifiers) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isProbe(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			v := vs.plain
			if sqsapi.ProtocolOf(r) == sqsapi.JSON {
				v = vs.json
			}
			verified, err := v.Verify(r)
			if err != nil {
				writeError(w, r, err)
				return
			}
			next.ServeHTTP(w, verified)
		})
	}
}

func isProbe(path string) bool {
	return path == health.HealthPath || path == health.ReadyPath
}

// writeError renders err for middleware: as an inspection error under the
// inspection prefix, else in the request's SQS protocol.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	if strings.HasPrefix(r.URL.Path, inspect.Prefix) {
		inspect.WriteError(w, r, err)
		return
	}
	sqsapi.WriteError(w, r, sqsapi.ProtocolOf(r), err)
}

// InspectAction is the action the inspection API needs, which is Tule's,
// not AWS's: a principal other than the root user may use any route under
// /_queues/ only when its identity policies allow tule:Inspect on
// InspectResource.
const InspectAction = "tule:Inspect"

// InspectResource is the resource tule:Inspect is evaluated on: every queue
// of the account and region, arn:aws:sqs:<region>:<account>:*, which a
// policy grants with that ARN, a wider pattern such as arn:aws:sqs:*:*:*,
// or "*".
func InspectResource(q *contracts.QueueSettings) string {
	return q.ARN("*")
}

// authorizeInspection refuses an inspection request whose principal may not
// inspect, with AccessDenied in the inspection API's error form. The SQS
// routes are authorized per operation by the queues module.
func authorizeInspection(
	authorizer *authz.Authorizer, q *contracts.QueueSettings,
) func(http.Handler) http.Handler {
	resource := InspectResource(q)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, inspect.Prefix) {
				next.ServeHTTP(w, r)
				return
			}
			p, ok := sigv4.PrincipalFrom(r.Context())
			if !ok {
				inspect.WriteError(w, r, errors.New(
					"authorize inspection: the request carries no verified principal"))
				return
			}
			err := authorizer.Authorize(
				r,
				p,
				authz.Check{Action: InspectAction, Resource: resource},
			)
			if err != nil {
				inspect.WriteError(w, r, err)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
