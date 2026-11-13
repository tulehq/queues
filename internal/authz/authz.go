// Package authz evaluates a request's principal against its identity
// policies and the queue's resource-based policy, as AWS evaluates them
// within one account and across accounts, with tule.dev/policy's
// Authorize. The root user of the service's account, the configured key or
// identity's root user, is always allowed and never evaluated; any other
// principal is evaluated on every check.
package authz

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"tule.dev/policy"
	"tule.dev/sigv4"

	"tule.dev/queues/internal/contracts"
)

// maxCachedPolicies bounds the parsed documents kept between requests. A
// document is parsed once and kept under its kind and the hash of its
// bytes; when the cache is full an arbitrary entry makes room.
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

// policyKind tells the cache's identity policies from its resource
// policies, which parse under different rules.
type policyKind byte

const (
	kindIdentity policyKind = iota
	kindResource
)

type cacheKey struct {
	hash [sha256.Size]byte
	kind policyKind
}

// Authorizer evaluates principals, keeping their parsed policies.
type Authorizer struct {
	now     func() time.Time
	cache   map[cacheKey]*policy.Policy
	account string
	region  string
	mu      sync.Mutex
}

// New returns an Authorizer for the resources of account in region that
// reads aws:CurrentTime and aws:EpochTime from now.
func New(account, region string, now func() time.Time) *Authorizer {
	return &Authorizer{
		now: now, account: account, region: region,
		cache: map[cacheKey]*policy.Policy{},
	}
}

// Check is one action on one resource a request needs, with the context
// keys only it has, such as aws:RequestTag/<key>, and the resource's
// resource-based policy.
type Check struct {
	Context map[string][]string
	// Policy is the resource-based policy of the resource, from
	// ResourcePolicy, or nil when the resource has none or the check does
	// not take one.
	Policy   *policy.Policy
	Action   string
	Resource string
	// Unshared marks an action a resource-based policy grants only to
	// principals of the resource's account. A principal of another account
	// is evaluated with Policy as usual, so a Deny in it applies, but an
	// Allow is denied for want of a resource-based policy allowing it.
	Unshared bool
}

// Denied is a check the principal may not make, and why, as
// tule.dev/policy's Authorize reports it.
type Denied struct {
	Principal string
	Action    string
	Resource  string
	Reason    policy.Reason
}

func (d *Denied) Error() string {
	return Message(d.Principal, d.Action, d.Resource, d.Reason)
}

// Message is AWS's AccessDenied message for reason: "User: <arn> is not
// authorized to perform: <action> on resource: <arn>" followed by the
// reason's phrase, "because no identity-based policy allows the <action>
// action", "because no resource-based policy allows the <action> action",
// "with an explicit deny in an identity-based policy" or "with an explicit
// deny in a resource-based policy" (IAM User Guide, "Troubleshoot access
// denied error messages"; "Troubleshoot an access denied error in Amazon
// SQS", SQS Developer Guide).
func Message(principal, action, resource string, reason policy.Reason) string {
	return "User: " + principal + " is not authorized to perform: " + action +
		" on resource: " + resource + " " + reason.Message(action)
}

// IsRoot reports whether p is the root user of the authorizer's account,
// which is never evaluated. Root alone is not enough: a Root principal of
// another account is root of that account only, and is evaluated like any
// principal of another account. A principal identity hands over in a state
// it never produces (checkPrincipal) is an error, root or not, so Root on
// a principal of another type never skips evaluation.
func (a *Authorizer) IsRoot(p sigv4.Principal) (bool, error) {
	if err := checkPrincipal(p); err != nil {
		return false, err
	}
	return p.Root && p.AccountID == a.account, nil
}

// Caller is the tule.dev/policy descriptor of p, from the attributes
// identity's key source hands over (sigv4 v0.4.1's Principal fields of the
// same names). Authorize supplies aws:PrincipalArn, aws:PrincipalAccount,
// aws:userid, aws:PrincipalType and aws:username from it.
func Caller(p sigv4.Principal) policy.Caller {
	return policy.Caller{
		AccountID:    p.AccountID,
		ARN:          p.ARN,
		PrincipalARN: p.PrincipalARN,
		UserID:       p.UserID,
		Type:         policy.PrincipalType(p.Type),
	}
}

// Authorize returns nil when p may make every check, a *Denied for the
// first it may not, or an error when p's policies cannot be read. The root
// user of the account is allowed without evaluation (IsRoot). Any other
// principal is decided by tule.dev/policy's Authorize on its identity
// policies and the check's resource-based policy, the resource being the
// account's: within the account either kind of Allow allows (a grant that
// names only the account also needs an identity Allow), across accounts
// both are needed, and an explicit Deny in either denies. An Unshared
// check of a principal of another account is never allowed.
func (a *Authorizer) Authorize(r *http.Request, p sigv4.Principal, checks ...Check) error {
	root, err := a.IsRoot(p)
	if err != nil || root {
		return err
	}
	pols, err := a.policies(p)
	if err != nil {
		return err
	}
	base := a.Context(r, p)
	for _, c := range checks {
		if err = a.check(p, base, c, pols); err != nil {
			return err
		}
	}
	return nil
}

// check decides one check of p, with the base context and p's parsed
// identity policies.
func (a *Authorizer) check(
	p sigv4.Principal, base map[string][]string, c Check, pols []*policy.Policy,
) error {
	ctx := base
	if len(c.Context) > 0 {
		ctx = maps.Clone(base)
		maps.Copy(ctx, c.Context)
	}
	az := policy.Authorize(policy.AuthorizeRequest{
		Action: c.Action, Resource: c.Resource, ResourceAccount: a.account,
		Caller: Caller(p), Context: ctx, Unknown: Unknown(p),
	}, c.Policy, pols...)
	if az.Decision == policy.Allow && c.Unshared && p.AccountID != a.account {
		// Only an Allow is replaced: an explicit Deny keeps its message.
		az.Decision, az.Reason = policy.ImplicitDeny, policy.NoResourceAllow
	}
	switch {
	case az.Decision == policy.Allow:
		return nil
	case az.Reason == policy.NoReason:
		return fmt.Errorf("authz: %s on %s denied (%v) without a reason",
			c.Action, c.Resource, az.Decision)
	}
	return &Denied{Principal: p.ARN, Action: c.Action, Resource: c.Resource, Reason: az.Reason}
}

// errPolicy wraps a policy identity handed over that this service cannot
// use. Identity checks every document with the same parser, so this is a
// fault between the two services, not the caller's.
var errPolicy = errors.New("authz: unusable identity policy")

// errPrincipal wraps a principal identity handed over that this service
// cannot evaluate: a fault between the two services, not the caller's.
var errPrincipal = errors.New("authz: inconsistent principal")

// errResourcePolicy wraps a stored queue policy that cannot be evaluated:
// one stored before this service checked policies, or one a later parser
// refuses. Every evaluated request on its queue fails until the root user
// replaces it (fail closed).
var errResourcePolicy = errors.New("authz: unusable resource-based policy")

// checkPrincipal refuses to evaluate p when identity handed over what it
// never does:
//
//   - Root and Type out of step. This service skips evaluation on Root
//     (IsRoot), while tule.dev/policy's Authorize gives full identity-based
//     access on a Type of Account, so a root user without Root, or Root on
//     another Type, would be judged by two different rules.
//   - Two tag keys that differ only by case: tule.dev/policy matches key
//     names regardless of case, so they would feed one
//     aws:PrincipalTag/<key> with two values.
func checkPrincipal(p sigv4.Principal) error {
	if (p.Type == sigv4.PrincipalTypeAccount) != p.Root {
		return fmt.Errorf("%w: %s has Root %t and Type %q", errPrincipal, p.ARN, p.Root, p.Type)
	}
	seen := make(map[string]string, len(p.Tags))
	for k := range p.Tags {
		lower := strings.ToLower(k)
		if other, dup := seen[lower]; dup {
			return fmt.Errorf("%w: %s has tag keys %q and %q, which differ only by case",
				errPrincipal, p.ARN, other, k)
		}
		seen[lower] = k
	}
	return nil
}

// ResourcePolicy parses a stored queue policy for Check.Policy, from the
// cache when it has it. "" is no policy, and nil. A document larger than
// SQS's 8,192 bytes (contracts.MaxPolicySize), which a policy stored
// before this service checked them may be, or one ParseResource refuses,
// is an error wrapping errResourcePolicy: the size bounds the work an
// evaluation does.
func (a *Authorizer) ResourcePolicy(doc string) (*policy.Policy, error) {
	if doc == "" {
		return nil, nil
	}
	if len(doc) > contracts.MaxPolicySize {
		return nil, fmt.Errorf("%w: %d bytes, over %d", errResourcePolicy, len(doc),
			contracts.MaxPolicySize)
	}
	pol, err := a.parse(kindResource, []byte(doc))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errResourcePolicy, err)
	}
	return pol, nil
}

// policies parses p's documents, from the cache when it has them.
func (a *Authorizer) policies(p sigv4.Principal) ([]*policy.Policy, error) {
	out := make([]*policy.Policy, 0, len(p.Policies))
	for i, doc := range p.Policies {
		if len(doc) > maxPolicySize {
			return nil, fmt.Errorf("%w: policy %d of %s is %d bytes, over %d",
				errPolicy, i, p.ARN, len(doc), maxPolicySize)
		}
		pol, err := a.parse(kindIdentity, doc)
		if err != nil {
			return nil, fmt.Errorf("%w: policy %d of %s: %w", errPolicy, i, p.ARN, err)
		}
		out = append(out, pol)
	}
	return out, nil
}

func (a *Authorizer) parse(kind policyKind, doc []byte) (*policy.Policy, error) {
	key := cacheKey{hash: sha256.Sum256(doc), kind: kind}
	a.mu.Lock()
	pol, ok := a.cache[key]
	a.mu.Unlock()
	if ok {
		return pol, nil
	}
	parse := policy.ParseIdentity
	if kind == kindResource {
		parse = policy.ParseResource
	}
	pol, err := parse(doc)
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped by the callers with what it is
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

// The organization keys, which identity supplies at signing-key version 4.
const (
	keyPrincipalOrgID    = "aws:PrincipalOrgID"
	keyPrincipalOrgPaths = "aws:PrincipalOrgPaths"
)

// Unknown returns the condition keys whose values are not known for p: the
// organization keys when identity could not tell p's organization
// (OrgUnknown), else none. They are passed as unknown, never left absent,
// since an absent key satisfies a negated operator: StringNotEquals on
// aws:PrincipalOrgID would grant, or let a Deny lapse, for a principal
// whose organization is not known.
func Unknown(p sigv4.Principal) []string {
	if p.OrgUnknown {
		return []string{keyPrincipalOrgID, keyPrincipalOrgPaths}
	}
	return nil
}

// Context returns the global condition keys of r by p: aws:SourceIp (the
// connection's address; nothing in front of the service is trusted),
// aws:CurrentTime and aws:EpochTime, aws:SecureTransport,
// aws:RequestedRegion (the service's region: AWS's endpoint fixes it,
// while here a request signed for any region verifies, so the credential
// scope's region is the caller's choice), aws:PrincipalTag/<key> for
// each of p's tags (an IAM user's, or a session's role's), and
// aws:PrincipalOrgID and aws:PrincipalOrgPaths when p's account is in the
// organization (left out otherwise, as AWS leaves them out, and named by
// Unknown when identity could not tell).
//
// The principal's own keys, aws:PrincipalArn (a role session's role ARN
// with its path), aws:PrincipalAccount, aws:userid, aws:PrincipalType and
// aws:username, are not here: tule.dev/policy's Authorize supplies them
// from Caller(p), replacing any value of them a context holds.
func (a *Authorizer) Context(r *http.Request, p sigv4.Principal) map[string][]string {
	now := a.now().UTC()
	ctx := map[string][]string{
		"aws:CurrentTime":     {now.Format("2006-01-02T15:04:05Z")},
		"aws:EpochTime":       {strconv.FormatInt(now.Unix(), 10)},
		"aws:SecureTransport": {strconv.FormatBool(r.TLS != nil)},
		"aws:RequestedRegion": {a.region},
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		ctx["aws:SourceIp"] = []string{host}
	}
	// Identity keeps a principal's tag keys unique regardless of case and
	// at most 50, so they feed distinct aws:PrincipalTag keys.
	for k, v := range p.Tags {
		ctx["aws:PrincipalTag/"+k] = []string{v}
	}
	if p.OrgID != "" {
		ctx[keyPrincipalOrgID] = []string{p.OrgID}
		ctx[keyPrincipalOrgPaths] = slices.Clone(p.OrgPaths)
	}
	return ctx
}
