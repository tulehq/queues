package app

import (
	"context"
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
	"tule.dev/queues/internal/modules/queues"
	"tule.dev/queues/internal/sqsapi"
	"tule.dev/queues/servicesend"
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

// verifiers are the SigV4 verifiers: one per protocol, and one for the
// service-send endpoint. A JSON request must sign X-Amz-Target, which names
// its operation, so a relay cannot turn a signed read into a delete with
// the same body; a Query request names its operation in the signed body,
// and an inspection request in the signed method and path. The internal
// one knows only the root principal's keys (rootOnly).
type verifiers struct {
	json     *sigv4.Verifier
	plain    *sigv4.Verifier
	internal *sigv4.Verifier
}

// newVerifiers returns the SigV4 verifiers every request but the probes goes
// through. They know the configured root key, belonging to the account's
// root user, and, when an identity endpoint is configured, every key
// identity issued, asked of identity on each request after the root key.
// What a key they accept may do is decided after them: the root key may do
// everything, and any other is evaluated against its identity policies.
// Requests are accepted signed for sqs in any region. The path is
// canonicalized as every service but S3 signs it, since Query requests go
// to queue URL paths. The internal one, for the service-send endpoint, is
// built the same way over rootOnly, and answers its failures as identity's
// internal endpoints answer theirs rather than as SQS errors.
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
	internal, err := sigv4.New(
		rootOnly{keys: keys, account: account},
		sigv4.WithBufferedPayload(maxBodySize),
		sigv4.WithRequiredSignedHeaders("host", "x-amz-date"),
		sigv4.WithService(servicesend.SigningName),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build the service-send verifier: %w", err)
	}
	return &verifiers{json: jsonV, plain: plainV, internal: internal}, nil
}

// rootOnly is the key source of the service-send endpoint: the keys of the
// root principal only, the principal the authorizer never evaluates
// (package authz), which is the configured key, or with identity a key
// identity issued the root user of the service's own account. Any other
// key is unknown, as identity's internal endpoints, which know the root
// user's key alone, answer one, so the endpoint refuses it before its body
// is read.
type rootOnly struct {
	keys    sigv4.KeySource
	account string
}

func (s rootOnly) SigningKey(ctx context.Context, scope sigv4.Scope) (sigv4.Key, error) {
	k, err := s.keys.SigningKey(ctx, scope)
	if err != nil {
		//nolint:wrapcheck // the key sources' errors are the sentinels the verifier reads
		return sigv4.Key{}, err
	}
	p := k.Principal
	if !p.Root || p.Type != sigv4.PrincipalTypeAccount || p.AccountID != s.account {
		return sigv4.Key{}, sigv4.ErrUnknownAccessKey
	}
	return k, nil
}

// verifySignature rejects requests whose SigV4 signature does not verify,
// and serves the rest as Verify returns them, with the signer's principal
// in their context and the body the signature covered. The service-send
// endpoint, its path exactly as sent, is verified against the root key alone, its failures answered
// as identity's internal endpoints answer theirs. Any other request
// carrying X-Amz-Target is verified as a JSON request, any other as a
// Query or inspection request.
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
			// The escaped path, which chi routes on: an encoded variant of
			// the path, such as /%5Finternal/send-message, decodes to it but
			// is routed elsewhere, so it is verified as any other request.
			if r.URL.EscapedPath() == servicesend.Path {
				verified, err := vs.internal.Verify(r)
				if err != nil {
					queues.WriteServiceSendError(w, r, err)
					return
				}
				next.ServeHTTP(w, verified)
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
