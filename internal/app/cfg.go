package app

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/gasmod/gas"
	"github.com/gasmod/gas/config"

	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/services/queues/sqlimpl"
)

// hstsSettings configures the Strict-Transport-Security header. It is only
// sent when MaxAge is set, so plain-HTTP deployments can leave it out.
type hstsSettings struct {
	MaxAge time.Duration
}

// authSettings holds the root key the server accepts. Every request must be
// SigV4-signed with it, so both key fields are required. IdentityEndpoint is
// the identity service's base URL, or empty to run standalone: set, a key
// identity issued is accepted too, and identity is asked with the root key,
// which must therefore be identity's root key as well.
type authSettings struct {
	AccessKeyID      string
	SecretAccessKey  string
	IdentityEndpoint string
}

// errNoCredentials is returned when the access key is not configured.
var errNoCredentials = errors.New("config: TULE_Auth_AccessKeyID and " +
	"TULE_Auth_SecretAccessKey must be set; every request is verified against them")

// Config is the bound application configuration. It keeps the store it was
// bound from, which the app registers as the gas.ConfigProvider so services
// can read settings Config itself does not model.
type Config struct {
	HSTS   *hstsSettings
	Auth   *authSettings
	Store  *sqlimpl.Settings        `validate:"required"`
	Queues *contracts.QueueSettings `validate:"required"`

	store gas.ConfigProvider
	now   func() time.Time
	Env   config.Environment
}

// SetClock makes now the clock the queues run on, in place of time.Now:
// message delays, visibility timeouts, retention periods, FIFO
// deduplication and receive-attempt windows, the PurgeQueue and DeleteQueue
// windows, and every timestamp the service reports. It is for tests, and is
// called between LoadConfigFrom and New.
//
// Signatures are still checked against the real time, so requests the SDK
// signs keep verifying however far the clock is moved. A long poll measures
// its wait in real time, and wakes when a message is sent or made visible
// and when the next delayed or in-flight message falls due by now as it
// stood when the poll last looked: a receive already waiting does not see
// the clock move. Move the clock, then receive.
func (c *Config) SetClock(now func() time.Time) {
	c.now = now
}

// clock returns the queues' clock: the one SetClock set, or time.Now.
func (c *Config) clock() func() time.Time {
	if c.now != nil {
		return c.now
	}
	return time.Now
}

const (
	envVarSeparator = "_"
	envVarPrefix    = "TULE_"
)

// LoadConfig loads configuration from a .env file and then the process
// environment, the later source winning.
func LoadConfig() (*Config, error) {
	return LoadConfigFrom(
		config.NewDotEnvProvider(
			config.WithDotEnvSeparator(envVarSeparator),
			config.WithDotEnvFileNotFoundPanic(false),
		),
		config.NewEnvProvider(
			config.WithEnvSeparator(envVarSeparator),
			config.WithEnvPrefix(envVarPrefix),
		),
	)
}

// LoadConfigFrom loads configuration from the given providers in order, then
// applies the same defaults and binding as LoadConfig. Tests use it to inject
// a provider of their own.
func LoadConfigFrom(provs ...config.Provider) (*Config, error) {
	env := config.NewEnvExtension()

	opts := make([]config.Option, 0, len(provs)+1)
	opts = append(opts, config.WithExtension(env))

	for _, p := range provs {
		opts = append(opts, config.WithProvider(p))
	}

	store := config.New(opts...)

	if err := store.Load(); err != nil {
		return nil, fmt.Errorf("config: load: %w", err)
	}

	return bindConfig(store, env.Current())
}

func bindConfig(store gas.ConfigProvider, env config.Environment) (*Config, error) {
	if err := applyDefaults(store); err != nil {
		return nil, err
	}

	cfg := Config{Env: env, store: store}
	if err := store.Bind(&cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if cfg.Auth == nil || cfg.Auth.AccessKeyID == "" || cfg.Auth.SecretAccessKey == "" {
		return nil, errNoCredentials
	}
	if err := checkIdentityEndpoint(cfg.Auth.IdentityEndpoint); err != nil {
		return nil, err
	}
	if err := checkQueues(cfg.Queues); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// checkIdentityEndpoint holds the identity endpoint, when set, to an
// absolute http or https URL.
func checkIdentityEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("config: Auth.IdentityEndpoint %q is not an http or https URL",
			endpoint)
	}
	return nil
}

var (
	accountIDPattern = regexp.MustCompile(`^\d{12}$`)
	// regionPattern is the shape of an AWS region name, such as us-east-1
	// or us-gov-west-1.
	regionPattern = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d+$`)
)

// checkQueues holds the account to 12 digits, the region to an AWS
// region's shape, and the base URL, when set, to an absolute http or https
// URL with nothing after the host but /.
func checkQueues(q *contracts.QueueSettings) error {
	if !accountIDPattern.MatchString(q.AccountID) {
		return fmt.Errorf("config: Queues.AccountID %q is not 12 digits", q.AccountID)
	}
	if !regionPattern.MatchString(q.Region) {
		return fmt.Errorf("config: Queues.Region %q is not an AWS region name", q.Region)
	}
	return checkBaseURL(q.BaseURL)
}

func checkBaseURL(base string) error {
	if base == "" {
		return nil
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || !hostOnly(u) {
		return fmt.Errorf("config: Queues.BaseURL %q is not an http or https URL with a "+
			"host and nothing after it but /", base)
	}
	return nil
}

// hostOnly reports whether u has a host and nothing after it but /.
func hostOnly(u *url.URL) bool {
	return u.Host != "" && (u.Path == "" || u.Path == "/") && u.RawQuery == "" &&
		u.Fragment == "" && u.User == nil
}

func applyDefaults(store gas.ConfigProvider) error {
	var errs []error
	setDefault := func(key string, value any) {
		if err := store.SetDefault(key, value); err != nil {
			errs = append(errs, fmt.Errorf("config: default %s: %w", key, err))
		}
	}

	// 9050, beside the ports the other Tule services use (object-store 9000,
	// secret-store 9010, accounts 9020, identity 9030), so all can run at
	// once.
	setDefault("Server.Port", 9050)

	// The server timeouts are set here rather than left to the gas defaults
	// (5s read, 10s write), which are too short for a signed request: the
	// signature covers the whole body, so a read cut short mid-body surfaces
	// to the client as a signature failure or a broken connection rather than
	// as a timeout. A request body may be up to maxBodySize, 4 MiB, and 30s
	// covers that at about 1.1 Mbit/s. The write timeout runs from the end
	// of the request's headers, so it must hold a ReceiveMessage's longest
	// wait, 20s, before the response is written; 30s does. IdleTimeout keeps the SDKs'
	// connection reuse working between calls, and matches the gas default; it
	// is set explicitly so it is visible here rather than inherited.
	setDefault("Server.ReadTimeout", "30s")
	setDefault("Server.WriteTimeout", "30s")
	setDefault("Server.IdleTimeout", "120s")

	// The headers get the same 30s as the whole request. gas requires
	// ReadHeaderTimeout to be positive, so it cannot be unlimited even when
	// ReadTimeout is 0, and its gas default of 5s would cut a slow client's
	// headers short of the budget above.
	setDefault("Server.ReadHeaderTimeout", "30s")

	// ShutdownTimeout is shorter than the gas default of 30s, which is a
	// promise a container runtime does not keep: `docker stop` sends SIGTERM
	// and kills the process 10s later. Every request here is one small SQLite
	// transaction, so in-flight work needs far less; a long poll in flight
	// is cut short, as a client's own timeout would cut it.
	//
	// It is not shorter than 7s because of a connection that was opened but
	// never sent a request, as an SDK client's spare one is. net/http closes
	// such a connection only once its whole-second timestamp is 6 seconds
	// old, 5s to 6s after it opened, and checks at most every 500ms, so the
	// shutdown can wait up to about 6.5s for it. With a shorter timeout gas
	// reports the deadline as an error and the process exits 1. Gas's
	// App.Stop returns within the timeout plus up to three 1s grace periods,
	// which only run while requests are still in flight: 10s at the very
	// worst, the edge of what docker gives.
	setDefault("Server.ShutdownTimeout", "7s")

	setDefault("Store.Path", "./data/queues.db")

	// Unset, queues runs standalone, on the root key alone.
	setDefault("Auth.IdentityEndpoint", "")

	// The account and region queue URLs and ARNs carry. The account is the
	// other services' default account, so the ARNs agree out of the box.
	setDefault("Queues.AccountID", "000000000000")
	setDefault("Queues.Region", "us-east-1")
	// Unset, a queue URL starts with the scheme and host the request came
	// in on.
	setDefault("Queues.BaseURL", "")

	return errors.Join(errs...)
}
