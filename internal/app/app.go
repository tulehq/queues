// Package app loads the configuration and wires the services, modules and
// middleware into a gas application.
package app

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gasmod/gas"
	"github.com/gasmod/gas/database"
	"github.com/gasmod/gas/migrate"
	"tule.dev/identity/principals"

	"tule.dev/queues/internal/authz"
	"tule.dev/queues/internal/contracts"
	"tule.dev/queues/internal/httpx"
	"tule.dev/queues/internal/modules/health"
	"tule.dev/queues/internal/modules/inspect"
	"tule.dev/queues/internal/modules/queues"
	"tule.dev/queues/internal/services/queues/sqlimpl"
)

// New builds the application from cfg. extra options are applied after the
// app's own. The queues run on the clock cfg.SetClock set, or on time.Now.
func New(cfg *Config, extra ...gas.Option) (*gas.App, error) {
	logger, lErr := newLogger(cfg.Env)
	if lErr != nil {
		return nil, lErr
	}

	// SQLite creates the database file but not the directory it sits in.
	if err := os.MkdirAll(filepath.Dir(cfg.Store.Path), 0o700); err != nil {
		return nil, fmt.Errorf("failed to create the store directory: %w", err)
	}

	// Every request is authenticated by its signature, not by a cookie, so
	// the cross-origin protection meant for browser sessions would reject
	// every call an SDK makes.
	opts := append([]gas.Option{gas.WithCSRFInsecureBypassPattern("/")}, extra...)
	app := gas.NewApp(opts...)

	// Policies are evaluated on the real time, as signatures are checked on
	// it, not on the clock SetClock sets for the queues.
	authorizer := authz.New(cfg.Queues.AccountID, cfg.Queues.Region, time.Now)

	pp, err := newPolicyPrincipals(cfg)
	if err != nil {
		return nil, err
	}

	registerInfraServices(app, cfg, logger)
	registerAppServices(app, cfg)
	registerAppModules(app, cfg, authorizer, pp)

	verifiers, err := newVerifiers(cfg)
	if err != nil {
		return nil, err
	}
	installMiddleware(app.Router(), cfg, logger, verifiers, authorizer)

	return app, nil
}

func registerInfraServices(app *gas.App, cfg *Config, logger gas.Logger) {
	app.RegisterServiceInstance[gas.ConfigProvider](cfg.store)
	app.RegisterServiceInstance[gas.Logger](logger)
	app.RegisterScopedService[httpx.RequestLogger](resolveRequestLogger)
	app.RegisterSingletonService[gas.DatabaseProvider](
		database.New(database.WithConfig(databaseConfig(cfg.Store))))
	app.RegisterSingletonService[gas.MigrationManager](migrate.New())
}

// databaseConfig is the gas/database configuration of the store's SQLite
// file.
//
// The pool holds one connection. SQLite lets one writer in at a time, and a
// second connection that starts a transaction as a reader and then writes
// fails with SQLITE_BUSY at once rather than waiting, so every transaction
// goes through the one connection and waits its turn there. Each operation
// is one short transaction, so the queue is short.
func databaseConfig(store *sqlimpl.Settings) *database.Config {
	cfg := database.DefaultConfig()
	cfg.Database.Dialect = database.DialectSQLite
	cfg.Database.DSN = gas.Secret("file:" + store.Path)
	cfg.Database.MaxOpenConns = 1
	cfg.Database.MaxIdleConns = 1
	// The one connection is kept for as long as gas/database allows, rather
	// than reopened every 30 minutes, since nothing is gained by recycling a
	// connection to a local file.
	cfg.Database.ConnMaxLifetime = 2 * time.Hour
	cfg.Database.ConnMaxIdleTime = time.Hour
	return cfg
}

func registerAppServices(app *gas.App, cfg *Config) {
	app.RegisterSingletonService[contracts.QueueProvider](
		sqlimpl.NewService(cfg.Queues, cfg.clock()))
}

func registerAppModules(app *gas.App, cfg *Config, authorizer *authz.Authorizer,
	pp contracts.PolicyPrincipals,
) {
	app.RegisterSingletonService[*queues.Module](queues.NewModule(cfg.Queues, authorizer, pp))
	app.RegisterSingletonService[*inspect.Module](inspect.NewModule(cfg.Queues, pp))
	app.RegisterSingletonService[*health.Module](health.NewModule)
}

// newPolicyPrincipals returns identity's principals client, which binds the
// queue policies' principals to unique IDs when they are stored and shows
// them as ARNs when they are read, or nil when queues runs standalone,
// storing and showing them as sent. It asks identity with the root key, as
// the key lookups do, and each call is bounded by identityTimeout.
func newPolicyPrincipals(cfg *Config) (contracts.PolicyPrincipals, error) {
	if cfg.Auth.IdentityEndpoint == "" {
		return nil, nil
	}
	c, err := principals.New(principals.Config{
		HTTPClient:      &http.Client{Timeout: identityTimeout},
		Endpoint:        cfg.Auth.IdentityEndpoint,
		AccessKeyID:     cfg.Auth.AccessKeyID,
		SecretAccessKey: cfg.Auth.SecretAccessKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build the principals client: %w", err)
	}
	return c, nil
}

func installMiddleware(
	r *gas.Router, cfg *Config, logger gas.Logger, verifiers *verifiers,
	authorizer *authz.Authorizer,
) {
	r.UseMiddlewareFunc(nil, httpx.RequestID(cfg.Env))

	// No X-Forwarded-For handling: nothing is expected in front of this
	// service, and the header is unsigned, so trusting a hop would only let a
	// caller choose the client IP that ends up in the logs.

	r.UseMiddlewareFunc(nil, withRequestLogger(logger))

	r.UseMiddlewareFunc(nil, gas.RequestLogger[httpx.RequestLogger]())

	r.UseMiddlewareFunc(nil, verifySignature(verifiers))

	r.UseMiddlewareFunc(nil, authorizeInspection(authorizer, cfg.Queues))

	var hsts string
	if cfg.HSTS != nil && cfg.HSTS.MaxAge > 0 {
		hsts = fmt.Sprintf("max-age=%0.f", cfg.HSTS.MaxAge.Seconds())
	}
	r.UseMiddlewareFunc(nil, gas.SecurityHeaders(
		gas.WithSecurityHeadersStrictTransportSecurity(hsts),
	))

	installUnrouted(r)
}
