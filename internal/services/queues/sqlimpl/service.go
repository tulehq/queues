// Package sqlimpl implements contracts.QueueProvider on a SQL database,
// SQLite in practice, reached through gas/database, with its schema applied
// by gas/migrate.
//
// Each operation runs in one transaction. Every time comes from the
// service's clock, so the delays, visibility timeouts, retention periods and
// windows SQS has follow it, and a test can drive them with a clock of its
// own.
//
// Message move tasks run in the background, one goroutine each, which the
// service stops when it closes.
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
	logger     gas.Logger
	now        func() time.Time
	signals    *signals
	movers     *movers
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
	logger gas.Logger,
) *Service {
	return func(db gas.DatabaseProvider, migrations gas.MigrationManager,
		bus *gas.EventBus, logger gas.Logger,
	) *Service {
		return &Service{
			db:         db,
			migrations: migrations,
			bus:        bus,
			logger:     logger,
			settings:   settings,
			now:        now,
			signals:    newSignals(),
			movers:     newMovers(),
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
// would otherwise hold the shutdown for up to its whole wait. It starts the
// move tasks' movers under a new run, so the tasks an earlier run, or this
// service before a restart (gas's RestartService runs Init again), left
// running are known.
func (s *Service) Init(context.Context) error {
	run, err := newNonce()
	if err != nil {
		return err
	}
	s.movers.begin(run)
	s.migrations.RegisterSlice(s, migrations)
	s.bus.SubscribeWithOwner[gas.SystemServerShuttingDown](s,
		func(gas.SystemServerShuttingDownPayload) { s.signals.stop() })
	return nil
}

// Close stops the move tasks running and waits for them, up to ctx's end.
// The database belongs to the database service, which closes after this
// one; a task stopped here is left running in the store, and the next run
// reports it failed. A task started once Close has begun is refused.
func (s *Service) Close(ctx context.Context) error {
	if err := s.movers.stop(ctx); err != nil {
		return fmt.Errorf("%s: stop the message move tasks: %w", s.Name(), err)
	}
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
