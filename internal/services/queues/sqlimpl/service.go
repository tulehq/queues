// Package sqlimpl implements contracts.QueueProvider on a SQL database,
// SQLite in practice, reached through gas/database, with its schema applied
// by gas/migrate.
//
// Each operation runs in one transaction. Every time comes from the
// service's clock, so the delays, visibility timeouts, retention periods and
// windows SQS has follow it, and a test can drive them with a clock of its
// own.
package sqlimpl

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gasmod/gas"

	"tule.dev/queues/internal/contracts"
)

// Service stores the queues in the database db provides.
type Service struct {
	db         gas.DatabaseProvider
	migrations gas.MigrationManager
	settings   *contracts.QueueSettings
	bus        *gas.EventBus
	now        func() time.Time
	signals    *signals
}

var (
	_ contracts.QueueProvider = (*Service)(nil)
	_ gas.Service             = (*Service)(nil)
)

// NewService returns the constructor gas uses to build a Service for the
// queues of settings' account and region, on the clock now.
func NewService(settings *contracts.QueueSettings, now func() time.Time) func(
	db gas.DatabaseProvider,
	migrations gas.MigrationManager,
	bus *gas.EventBus,
) *Service {
	return func(db gas.DatabaseProvider, migrations gas.MigrationManager,
		bus *gas.EventBus,
	) *Service {
		return &Service{
			db:         db,
			migrations: migrations,
			bus:        bus,
			settings:   settings,
			now:        now,
			signals:    newSignals(),
		}
	}
}

// Name identifies the service to gas.
func (s *Service) Name() string {
	return "tule/services/queues/sql"
}

// Init registers the schema migrations, which gas applies once every service
// is initialized, and ends the long polls in flight when the server begins
// to shut down: net/http waits for every request to finish, and a poll
// would otherwise hold the shutdown for up to its whole wait.
func (s *Service) Init(context.Context) error {
	s.migrations.RegisterSlice(s, migrations)
	s.bus.SubscribeWithOwner[gas.SystemServerShuttingDown](s,
		func(gas.SystemServerShuttingDownPayload) { s.signals.stop() })
	return nil
}

// Close releases nothing; the database belongs to the database service.
func (s *Service) Close(context.Context) error {
	return nil
}

// inTx runs fn in one transaction, which commits only when fn returns nil.
func (s *Service) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if err := s.db.WithTx(ctx, nil, fn); err != nil {
		return fmt.Errorf("%s: %w", s.Name(), err)
	}
	return nil
}

// stamp is how the store keeps a time: Unix milliseconds.
func stamp(t time.Time) int64 {
	return t.UnixMilli()
}

func unstamp(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

// unstampOrZero is unstamp for a column where 0 means never.
func unstampOrZero(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return unstamp(ms)
}
