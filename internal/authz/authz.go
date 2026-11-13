// Package authz evaluates a request's principal against its identity
// policies, as AWS evaluates identity-based policies within one account,
// with tule.dev/policy. A root principal, the configured key or identity's
// root user, is always allowed and never evaluated; any other is evaluated
// on every check. A principal of another account is refused even when its
// identity policies allow the check: AWS also needs a resource-based policy
// to grant a cross-account request, and none is enforced here.
package authz

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"tule.dev/policy"
	"tule.dev/sigv4"
)

// maxCachedPolicies bounds the parsed documents kept between requests. A
// document is parsed once and kept under the hash of its bytes; when the
// cache is full an arbitrary entry makes room.
const maxCachedPolicies = 1024

// maxPolicySize bounds a document identity hands over. Identity holds a
// managed policy to 6,144 characters and a role's inline policies to
// 10,240 in all, without white space; 64 KiB holds either with any white
// space, and keeps a faulty identity from making a request parse megabytes.
const maxPolicySize = 64 << 10

// MaxContextValues bounds the values of a request-derived multivalued
// context key, such as aws:TagKeys. tule.dev/policy's cost grows with the
// context's size times the policies', so the caller refuses a request with
// more before evaluating it. 50 is SQS's tag quota per queue.
const MaxContextValues = 50

// Authorizer evaluates principals, keeping their parsed policies.
type Authorizer struct {
	now     func() time.Time
	cache   map[[sha256.Size]byte]*policy.Policy
	account string
	region  string
	mu      sync.Mutex
}

// New returns an Authorizer for the resources of account in region that
// reads aws:CurrentTime and aws:EpochTime from now.
func New(account, region string, now func() time.Time) *Authorizer {
	return &Authorizer{
		now: now, account: account, region: region,
		cache: map[[sha256.Size]byte]*policy.Policy{},
	}
}

// Check is one action on one resource a request needs, with the context
// keys only it has, such as aws:RequestTag/<key>.
type Check struct {
	Context  map[string][]string
	Action   string
	Resource string
}

// Reason is why a check is denied.
type Reason int

// The reasons, each with its AccessDenied message.
const (
	// NoIdentityAllow: no identity policy allows the check.
	NoIdentityAllow Reason = iota
	// ExplicitDeny: an identity policy denies it.
	ExplicitDeny
	// NoResourceAllow: the identity policies allow a check of another
	// account's principal, which needs a resource-based policy too.
	NoResourceAllow
)

// Denied is a check the principal may not make.
type Denied struct {
	Principal string
	Action    string
	Resource  string
	Reason    Reason
}

func (d *Denied) Error() string {
	return Message(d.Principal, d.Action, d.Resource, d.Reason)
}

// Message is AWS's AccessDenied message for reason: "User: <arn> is not
// authorized to perform: <action> on resource: <arn> because no
// identity-based policy allows the <action> action", "... with an explicit
// deny in an identity-based policy", or "... because no resource-based
// policy allows the <action> action".
func Message(principal, action, resource string, reason Reason) string {
	head := "User: " + principal + " is not authorized to perform: " + action +
		" on resource: " + resource
	switch reason {
	case ExplicitDeny:
		return head + " with an explicit deny in an identity-based policy"
	case NoResourceAllow:
		return head + " because no resource-based policy allows the " + action + " action"
	default:
		return head + " because no identity-based policy allows the " + action + " action"
	}
}

// IsRoot reports whether p is the root user of the authorizer's account,
// which is never evaluated. Root alone is not enough: a Root principal of
// another account is root of that account only, and is evaluated like any
// principal of another account.
func (a *Authorizer) IsRoot(p sigv4.Principal) bool {
	return p.Root && p.AccountID == a.account
}

// Authorize returns nil when p may make every check, a *Denied for the
// first it may not, or an error when p's policies cannot be read. The root
// user of the account is allowed without evaluation (IsRoot). A principal of another account
// is evaluated first, so an explicit deny or a missing allow is reported as
// such, and is then denied for want of a resource-based policy.
func (a *Authorizer) Authorize(r *http.Request, p sigv4.Principal, checks ...Check) error {
	if a.IsRoot(p) {
		return nil
	}
	pols, err := a.policies(p)
	if err != nil {
		return err
	}
	base := a.Context(r, p)
	for _, c := range checks {
		ctx := base
		if len(c.Context) > 0 {
			ctx = maps.Clone(base)
			maps.Copy(ctx, c.Context)
		}
		res := policy.Evaluate(policy.Request{
			Action: c.Action, Resource: c.Resource, Principal: p.ARN, Context: ctx,
		}, pols...)
		reason := NoIdentityAllow
		switch {
		case res.Decision == policy.ExplicitDeny:
			reason = ExplicitDeny
		case res.Decision == policy.Allow && p.AccountID == a.account:
			continue
		case res.Decision == policy.Allow:
			reason = NoResourceAllow
		}
		return &Denied{
			Principal: p.ARN, Action: c.Action, Resource: c.Resource, Reason: reason,
		}
	}
	return nil
}

// errPolicy wraps a policy identity handed over that this service cannot
// use. Identity checks every document with the same parser, so this is a
// fault between the two services, not the caller's.
var errPolicy = errors.New("authz: unusable identity policy")

// policies parses p's documents, from the cache when it has them.
func (a *Authorizer) policies(p sigv4.Principal) ([]*policy.Policy, error) {
	out := make([]*policy.Policy, 0, len(p.Policies))
	for i, doc := range p.Policies {
		if len(doc) > maxPolicySize {
			return nil, fmt.Errorf("%w: policy %d of %s is %d bytes, over %d",
				errPolicy, i, p.ARN, len(doc), maxPolicySize)
		}
		pol, err := a.parse(doc)
		if err != nil {
			return nil, fmt.Errorf("%w: policy %d of %s: %w", errPolicy, i, p.ARN, err)
		}
		out = append(out, pol)
	}
	return out, nil
}

func (a *Authorizer) parse(doc []byte) (*policy.Policy, error) {
	key := sha256.Sum256(doc)
	a.mu.Lock()
	pol, ok := a.cache[key]
	a.mu.Unlock()
	if ok {
		return pol, nil
	}
	pol, err := policy.ParseIdentity(doc)
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by policies with the principal
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.cache) >= maxCachedPolicies {
		for k := range a.cache {
			delete(a.cache, k)
			break
		}
	}
	a.cache[key] = pol
	return pol, nil
}

// The principal types aws:PrincipalType takes (IAM User Guide, "AWS global
// condition context keys").
const (
	typeUser        = "User"
	typeAssumedRole = "AssumedRole"
	typeAccount     = "Account"
)

// Context returns the global condition keys of r by p: aws:SourceIp (the
// connection's address; nothing in front of the service is trusted),
// aws:CurrentTime and aws:EpochTime, aws:SecureTransport,
// aws:RequestedRegion (the service's region: AWS's endpoint fixes it,
// while here a request signed for any region verifies, so the credential
// scope's region is the caller's choice),
// aws:PrincipalAccount, aws:PrincipalArn, aws:PrincipalType, aws:username
// for an IAM user and aws:userid for the root user.
//
// A role session's aws:PrincipalArn is its role's ARN without the role's
// path, which a session ARN does not carry and identity does not hand over;
// AWS's has the path.
func (a *Authorizer) Context(r *http.Request, p sigv4.Principal) map[string][]string {
	now := a.now().UTC()
	ctx := map[string][]string{
		"aws:CurrentTime":      {now.Format("2006-01-02T15:04:05Z")},
		"aws:EpochTime":        {strconv.FormatInt(now.Unix(), 10)},
		"aws:SecureTransport":  {strconv.FormatBool(r.TLS != nil)},
		"aws:PrincipalAccount": {p.AccountID},
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		ctx["aws:SourceIp"] = []string{host}
	}
	ctx["aws:RequestedRegion"] = []string{a.region}
	for k, v := range principalKeys(p) {
		ctx[k] = []string{v}
	}
	return ctx
}

// principalKeys derives the principal's keys from its ARN:
// arn:aws:iam::<account>:root, arn:aws:iam::<account>:user/<path><name> or
// arn:aws:sts::<account>:assumed-role/<role>/<session>.
func principalKeys(p sigv4.Principal) map[string]string {
	keys := map[string]string{"aws:PrincipalArn": p.ARN}
	parts := strings.SplitN(p.ARN, ":", 6)
	if len(parts) != 6 {
		return keys
	}
	prefix, resource := strings.Join(parts[:5], ":"), parts[5]
	switch {
	case parts[2] == "iam" && resource == "root":
		keys["aws:PrincipalType"] = typeAccount
		keys["aws:userid"] = parts[4]
	case parts[2] == "iam" && strings.HasPrefix(resource, "user/"):
		keys["aws:PrincipalType"] = typeUser
		keys["aws:username"] = resource[strings.LastIndexByte(resource, '/')+1:]
	case parts[2] == "sts" && strings.HasPrefix(resource, "assumed-role/"):
		keys["aws:PrincipalType"] = typeAssumedRole
		role, _, ok := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")
		if ok && role != "" {
			iamPrefix := strings.Replace(prefix, ":sts:", ":iam:", 1)
			keys["aws:PrincipalArn"] = iamPrefix + ":role/" + role
		}
	}
	return keys
}
