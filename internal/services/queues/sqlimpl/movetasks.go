package sqlimpl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"tule.dev/queues/internal/contracts"
)

// moveBatch is the most messages one step of a move task moves, in one
// transaction, so a task holds the store's one connection only briefly.
const moveBatch = 10

// heldPause is how long a task waiting for held FIFO groups waits before
// it looks again: nothing signals it when a group's message in flight is
// deleted or its visibility ends.
const heldPause = 250 * time.Millisecond

// The failure reasons a move task reports.
const (
	reasonInterrupted = "The server stopped while the task was running."
	reasonInternal    = "The task failed with an internal error."
)

// errStopped is a task started while the service is closed: no goroutine
// would run it, so it is refused rather than left running in the store.
var errStopped = errors.New("sqlimpl: the service is closed; no move task can start")

// movers are the move tasks running in the background, one goroutine each.
// Each task has a channel that wakes it from its pause between steps, as
// a cancel does. The movers run from begin, which the service's Init
// calls, to stop, which its Close calls; gas's RestartService calls Init
// again on the same service, and begin starts them over under a new run.
type movers struct {
	ctx    context.Context //nolint:containedctx // the tasks' lifetime, ended by stop
	cancel context.CancelFunc
	wake   map[int64]chan struct{}
	// run tells this run's tasks from those of earlier runs.
	run string
	wg  sync.WaitGroup
	mu  sync.Mutex
}

// newMovers returns movers that are stopped until begin.
func newMovers() *movers {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &movers{ctx: ctx, cancel: cancel, wake: map[int64]chan struct{}{}}
}

// begin starts the movers under the run run. Tasks of the run before, which
// stop ended, are no longer moving anything (settleTasks).
func (m *movers) begin(run string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancel()
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.run = run
}

// current returns the run the movers run under.
func (m *movers) current() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.run
}

// ticket is a place claim holds for a task about to start: the run it
// belongs to and the lifetime it runs for.
type ticket struct {
	ctx context.Context //nolint:containedctx // the task's lifetime
	run string
}

// claim holds a place for a task about to start, which the caller must
// launch or release, and fails with errStopped once the movers are
// stopped. stop waits for the places held, so a task is never stored
// running in a run with no goroutine to run it.
func (m *movers) claim() (ticket, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx.Err() != nil {
		return ticket{}, errStopped
	}
	m.wg.Add(1)
	return ticket{ctx: m.ctx, run: m.run}, nil
}

// release gives back a place claim held for a task that did not start.
func (m *movers) release() {
	m.wg.Done()
}

// launch runs the task id, for which t was claimed, on a goroutine of its
// own. A task launched after stop began sees its lifetime ended and
// returns at once, left running in the store for the next run to settle.
func (m *movers) launch(t ticket, id int64, run func(ctx context.Context,
	woken <-chan struct{}),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	woken := make(chan struct{}, 1)
	m.wake[id] = woken
	go func() {
		defer m.wg.Done()
		defer func() {
			m.mu.Lock()
			delete(m.wake, id)
			m.mu.Unlock()
		}()
		run(t.ctx, woken)
	}()
}

// poke wakes the task id from its pause, when it runs.
func (m *movers) poke(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.wake[id]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// stop stops every task and waits for them, up to ctx's end.
func (m *movers) stop(ctx context.Context) error {
	m.mu.Lock()
	m.cancel()
	m.mu.Unlock()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Tasks that stopped as ctx ended count as stopped.
		select {
		case <-done:
			return nil
		default:
			return ctx.Err() //nolint:wrapcheck // Close wraps it
		}
	}
}

// settleTasks ends the tasks an earlier server run left active, since
// nothing moves their messages any more: a running one fails, and one
// being canceled is canceled. Every operation on the tasks runs it
// first, in its own transaction.
func (s *Service) settleTasks(ctx context.Context, tx *sql.Tx) error {
	run := s.movers.current()
	if err := exec(ctx, tx,
		`UPDATE move_tasks SET status = ?, failure_reason = ? WHERE status = ? AND run != ?`,
		contracts.MoveTaskFailed, reasonInterrupted, contracts.MoveTaskRunning,
		run); err != nil {
		return err
	}
	return exec(ctx, tx, `UPDATE move_tasks SET status = ? WHERE status = ? AND run != ?`,
		contracts.MoveTaskCancelled, contracts.MoveTaskCancelling, run)
}

// MoveScope returns the last message row of the queue and the names of the
// queues its messages up to that row came from, those of this account and
// region, in order.
func (s *Service) MoveScope(ctx context.Context, name string) (*contracts.MoveScope, error) {
	var out *contracts.MoveScope
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, _, err := openQueue(ctx, tx, name, s.now())
		if err != nil {
			return err
		}
		out = &contracts.MoveScope{}
		if _, err = scanOne(ctx, tx, []any{&out.UpTo},
			`SELECT COALESCE(MAX(id), 0) FROM messages WHERE queue_id = ?`, q.id); err != nil {
			return err
		}
		arns, err := queryAll(ctx, tx, scanString,
			`SELECT DISTINCT dlq_source_arn FROM messages
			 WHERE queue_id = ? AND id <= ? AND dlq_source_arn != '' ORDER BY dlq_source_arn`,
			q.id, out.UpTo)
		if err != nil {
			return err
		}
		for _, arn := range arns {
			if n, ok := s.settings.NameOfARN(arn); ok {
				out.Origins = append(out.Origins, n)
			}
		}
		return nil
	})
	return out, err
}

// StartMoveTask starts a task moving the source queue's messages up to
// in.UpTo, oldest first, and returns its handle. The source must be the
// dead-letter queue of a queue and have no task running or being
// canceled; a destination must exist, be of the source's type and not be
// the source. ApproximateNumberOfMessagesToMove is the number of the
// source's visible messages up to in.UpTo now.
func (s *Service) StartMoveTask(ctx context.Context, in contracts.StartMoveInput) (
	string, error,
) {
	var (
		id     int64
		handle string
	)
	t, err := s.movers.claim()
	if err != nil {
		return "", err
	}
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		var sErr error
		id, handle, sErr = s.startMoveTask(ctx, tx, in, t.run)
		return sErr
	})
	if err != nil {
		s.movers.release()
		return "", err
	}
	s.movers.launch(t, id, func(ctx context.Context, woken <-chan struct{}) {
		s.runMoveTask(ctx, id, woken)
	})
	return handle, nil
}

// startMoveTask is StartMoveTask's transaction: it checks the task and
// stores it, running in the run run.
func (s *Service) startMoveTask(ctx context.Context, tx *sql.Tx, in contracts.StartMoveInput,
	run string,
) (int64, string, error) {
	now := s.now()
	q, _, err := openQueue(ctx, tx, in.Source, now)
	if err != nil {
		return 0, "", err
	}
	if err = s.settleTasks(ctx, tx); err != nil {
		return 0, "", err
	}
	sources, err := s.sourcesOf(ctx, tx, q.name, "", 1)
	if err != nil {
		return 0, "", err
	}
	if len(sources) == 0 {
		return 0, "", contracts.ErrNotDeadLetterQueue
	}
	destination := ""
	if in.Destination != "" {
		if err = checkMoveDestination(ctx, tx, q, in.Destination); err != nil {
			return 0, "", err
		}
		destination = s.settings.ARN(in.Destination)
	}
	found, err := scanOne(ctx, tx, []any{new(int)},
		`SELECT 1 FROM move_tasks WHERE queue_id = ? AND status IN (?, ?)`,
		q.id, contracts.MoveTaskRunning, contracts.MoveTaskCancelling)
	if err != nil {
		return 0, "", err
	}
	if found {
		return 0, "", contracts.ErrMoveTaskActive
	}
	return s.insertMoveTask(ctx, tx, q, in, destination, run, now)
}

// checkMoveDestination checks a move task's destination queue name for the
// source q: it must exist, be of q's type, and not be q.
func checkMoveDestination(ctx context.Context, tx *sql.Tx, q *queueRow, name string) error {
	invalid := func(reason string) error {
		return &contracts.InvalidError{Field: "DestinationArn", Value: name, Reason: reason}
	}
	if name == q.name {
		return invalid("The destination queue cannot be the source queue")
	}
	dst, err := loadQueue(ctx, tx, name)
	if err != nil {
		return err
	}
	if dst.fifo != q.fifo {
		return invalid("The destination queue must be of the same type as the source queue")
	}
	return nil
}

// insertMoveTask stores a new running task of the run run, and returns its
// row and handle.
func (s *Service) insertMoveTask(ctx context.Context, tx *sql.Tx, q *queueRow,
	in contracts.StartMoveInput, destination, run string, now time.Time,
) (int64, string, error) {
	var toMove int64
	if _, err := scanOne(ctx, tx, []any{&toMove},
		`SELECT COUNT(*) FROM messages WHERE queue_id = ? AND id <= ? AND visible_at <= ?`,
		q.id, in.UpTo, stamp(now)); err != nil {
		return 0, "", err
	}
	taskID, err := newMessageID()
	if err != nil {
		return 0, "", err
	}
	handle := contracts.TaskHandle(taskID, s.settings.ARN(q.name))
	rate := 0
	if in.MaxPerSecond != nil {
		rate = *in.MaxPerSecond
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO move_tasks (queue_id, handle, destination_arn, max_per_second, status,
		   to_move, up_to, started_at, run)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		q.id, handle, destination, rate, contracts.MoveTaskRunning, toMove, in.UpTo,
		stamp(now), run)
	if err != nil {
		return 0, "", fmt.Errorf("insert move task: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, "", fmt.Errorf("insert move task: %w", err)
	}
	return id, handle, nil
}

// ListMoveTasks returns up to n of the queue's most recent tasks, the
// oldest first.
func (s *Service) ListMoveTasks(ctx context.Context, name string, n int) (
	[]*contracts.MoveTask, error,
) {
	var out []*contracts.MoveTask
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		if err = s.settleTasks(ctx, tx); err != nil {
			return err
		}
		source := s.settings.ARN(q.name)
		out, err = queryAll(ctx, tx, func(r scanner) (*contracts.MoveTask, error) {
			t := &contracts.MoveTask{SourceARN: source}
			var rate int
			var started int64
			if sErr := r.Scan(&t.Handle, &t.DestinationARN, &rate, &t.Status, &t.ToMove,
				&t.Moved, &t.FailureReason, &started); sErr != nil {
				return nil, sErr //nolint:wrapcheck // queryAll wraps it
			}
			if rate > 0 {
				t.MaxPerSecond = &rate
			}
			t.StartedAt = unstamp(started)
			return t, nil
		}, `SELECT handle, destination_arn, max_per_second, status, to_move, moved,
		      failure_reason, started_at
		    FROM move_tasks WHERE queue_id = ? ORDER BY id DESC LIMIT ?`, q.id, n)
		return err
	})
	slices.Reverse(out)
	return out, err
}

// CancelMoveTask cancels the running task of the handle on the queue name:
// the task is canceling until its goroutine, woken, stops between two
// steps and marks it canceled. The messages it moved stay moved.
func (s *Service) CancelMoveTask(ctx context.Context, name, handle string) (int64, error) {
	var id, moved int64
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		if err = s.settleTasks(ctx, tx); err != nil {
			return err
		}
		var status string
		found, err := scanOne(ctx, tx, []any{&id, &status, &moved},
			`SELECT id, status, moved FROM move_tasks WHERE queue_id = ? AND handle = ?`,
			q.id, handle)
		switch {
		case err != nil:
			return err
		case !found:
			return contracts.ErrMoveTaskNotFound
		case status != contracts.MoveTaskRunning:
			return contracts.ErrMoveTaskNotRunning
		}
		return exec(ctx, tx, `UPDATE move_tasks SET status = ? WHERE id = ?`,
			contracts.MoveTaskCancelling, id)
	})
	if err != nil {
		return 0, err
	}
	s.movers.poke(id)
	return moved, nil
}

// runMoveTask runs the task id until it ends or ctx does: a step at a time,
// pausing after each for as long as the task's rate asks of the messages it
// moved. A task without a rate does not pause. A cancel wakes it from its
// pause. A failure of the store fails the task.
func (s *Service) runMoveTask(ctx context.Context, id int64, woken <-chan struct{}) {
	for {
		began := time.Now()
		st, err := s.moveStep(ctx, id)
		if err != nil {
			if ctx.Err() == nil {
				s.failMoveTask(ctx, id, err)
			}
			return
		}
		if st.done {
			return
		}
		if !s.pauseMoveTask(ctx, woken, st, time.Since(began)) {
			return
		}
	}
}

// pauseMoveTask waits after a step that took took, for the rest of the
// time the task's rate gives the messages it moved, and at least heldPause
// when the task waits for held FIFO groups, or until a cancel wakes the
// task. It reports false when ctx ended.
func (s *Service) pauseMoveTask(ctx context.Context, woken <-chan struct{}, st stepped,
	took time.Duration,
) bool {
	var wait time.Duration
	if st.rate > 0 {
		wait = time.Duration(st.moved)*time.Second/time.Duration(st.rate) - took
	}
	if st.held {
		wait = max(wait, heldPause)
	}
	if wait <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
	case <-woken:
	case <-ctx.Done():
		return false
	}
	return true
}

// failMoveTask fails the task id for a failure of the store, err, which is
// logged.
func (s *Service) failMoveTask(ctx context.Context, id int64, err error) {
	s.logger.Error("message move task failed").Int64("task", id).Err("error", err).Send()
	if fErr := s.inTx(ctx, func(tx *sql.Tx) error {
		return exec(
			ctx,
			tx,
			`UPDATE move_tasks SET status = ?, failure_reason = ? WHERE id = ? AND status IN (?, ?)`,
			contracts.MoveTaskFailed,
			reasonInternal,
			id,
			contracts.MoveTaskRunning,
			contracts.MoveTaskCancelling,
		)
	}); fErr != nil {
		s.logger.Error("failed to mark a message move task failed").Int64("task", id).
			Err("error", fErr).Send()
	}
}

// stepped is what one step of a task did: how many messages it moved, the
// task's rate (0 for none), whether the task has ended, and whether it
// waits for FIFO groups held by a message in flight (heldBack).
type stepped struct {
	moved int
	rate  int
	done  bool
	held  bool
}

// moveTaskRow is a task as a step reads it.
type moveTaskRow struct {
	status      string
	destination string
	queueID     int64
	rate        int
	upTo        int64
}

// moveStep is one step of the task id, in one transaction: it moves up to
// moveBatch of the source's visible messages up to the task's last row,
// oldest first, at most the task's rate, and completes the task when none
// is left. A task being canceled is canceled; one that ended, or is gone
// with its queue, does nothing. The destinations are signaled once the
// step commits.
func (s *Service) moveStep(ctx context.Context, id int64) (stepped, error) {
	var (
		st    stepped
		names []string
	)
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		st, names = stepped{}, nil
		var t moveTaskRow
		found, err := scanOne(ctx, tx,
			[]any{&t.status, &t.destination, &t.queueID, &t.rate, &t.upTo},
			`SELECT status, destination_arn, queue_id, max_per_second, up_to
			 FROM move_tasks WHERE id = ?`, id)
		if err != nil || !found {
			st.done = true
			return err
		}
		st.rate = t.rate
		switch t.status {
		case contracts.MoveTaskRunning:
		case contracts.MoveTaskCancelling:
			st.done = true
			return exec(ctx, tx, `UPDATE move_tasks SET status = ? WHERE id = ?`,
				contracts.MoveTaskCancelled, id)
		default:
			st.done = true
			return nil
		}
		names, err = s.moveMessages(ctx, tx, id, &t, &st)
		return err
	})
	if err == nil && len(names) > 0 {
		s.signals.signal(names...)
	}
	return st, err
}

// moveMessages moves the step's messages of the running task t, records
// them, and ends the task when it completes or a message cannot be moved.
// It returns the names of the queues it moved messages to.
func (s *Service) moveMessages(ctx context.Context, tx *sql.Tx, id int64, t *moveTaskRow,
	st *stepped,
) ([]string, error) {
	now := s.now()
	n := moveBatch
	if t.rate > 0 {
		n = min(n, t.rate)
	}
	// One more than the step moves tells whether any is left after it.
	msgs, q, err := toMove(ctx, tx, t, n+1, now)
	if err != nil || q == nil {
		st.done = true
		return nil, err
	}
	var (
		names  []string
		status = contracts.MoveTaskRunning
		reason string
	)
	for _, m := range msgs[:min(n, len(msgs))] {
		var dest string
		if dest, reason, err = s.redrive(ctx, tx, t.destination, m, now); err != nil {
			return nil, err
		}
		if reason != "" {
			status = contracts.MoveTaskFailed
			break
		}
		names = append(names, dest)
		st.moved++
	}
	if status == contracts.MoveTaskRunning && len(msgs) <= n {
		if status, err = afterLast(ctx, tx, q, t, st, now); err != nil {
			return nil, err
		}
	}
	st.done = status != contracts.MoveTaskRunning
	return names, exec(ctx, tx,
		`UPDATE move_tasks SET moved = moved + ?, status = ?, failure_reason = ? WHERE id = ?`,
		st.moved, status, reason, id)
}

// heldFIFO keeps, of a FIFO source's visible messages, those a receive
// could take: a group is held while any of its messages is in flight, and
// a message that is not visible holds the messages behind it in its group,
// as takeFIFO holds them, so a task moves a group's messages in their
// order.
const heldFIFO = ` AND NOT EXISTS (
	SELECT 1 FROM messages x
	WHERE x.queue_id = messages.queue_id AND x.group_id = messages.group_id
	  AND x.visible_at > ?2 AND (x.receive_count > 0 OR x.id < messages.id))`

// toMove returns up to n of the visible messages up to the task t's last
// row, oldest first, of its source queue, whose expired messages it
// deletes first, and the queue, nil when it is gone. Of a FIFO queue it
// returns only those heldFIFO keeps.
func toMove(ctx context.Context, tx *sql.Tx, t *moveTaskRow, n int, now time.Time) (
	[]*messageRow, *queueRow, error,
) {
	rows, err := queryAll(ctx, tx, scanQueue,
		`SELECT `+queueColumns+` FROM queues WHERE id = ?`, t.queueID)
	if err != nil || len(rows) == 0 {
		return nil, nil, err
	}
	q := rows[0]
	set, err := q.settings()
	if err != nil {
		return nil, nil, err
	}
	if err = expire(ctx, tx, q, set, now); err != nil {
		return nil, nil, err
	}
	query := `SELECT ` + messageColumns + ` FROM messages
		WHERE queue_id = ?1 AND id <= ?3 AND visible_at <= ?2`
	if q.fifo {
		query += heldFIFO
	}
	msgs, err := queryAll(ctx, tx, scanMessage, query+` ORDER BY id LIMIT ?4`,
		q.id, stamp(now), t.upTo, n)
	return msgs, q, err
}

// afterLast is the status of a running task t whose step moved every
// message it could: COMPLETED, or RUNNING while it waits for held FIFO
// groups (heldBack), which it records in st.
func afterLast(ctx context.Context, tx *sql.Tx, q *queueRow, t *moveTaskRow, st *stepped,
	now time.Time,
) (string, error) {
	held, err := heldBack(ctx, tx, q, t, now)
	if err != nil || held {
		st.held = held
		return contracts.MoveTaskRunning, err
	}
	return contracts.MoveTaskCompleted, nil
}

// heldBack reports whether a FIFO source still has visible messages up to
// the task t's last row once a step moved every message it could: messages
// of groups heldFIFO holds, which the task waits for rather than leaves.
func heldBack(ctx context.Context, tx *sql.Tx, q *queueRow, t *moveTaskRow, now time.Time) (
	bool, error,
) {
	if !q.fifo {
		return false, nil
	}
	return scanOne(ctx, tx, []any{new(int)},
		`SELECT 1 FROM messages WHERE queue_id = ? AND id <= ? AND visible_at <= ? LIMIT 1`,
		q.id, t.upTo, stamp(now))
}

// redrive moves the message m to the queue of the ARN destination, or, when
// it is "", back to the queue it came from, and returns that queue's name.
// The message arrives as a new one, as the developer guide describes a
// redriven message: a new message ID and enqueue time, so its retention
// starts again, visible at once, never received; its body, attributes,
// sender and sent time are kept. A FIFO message keeps its group and
// deduplication ID and takes the destination's next sequence number.
//
// A message that cannot be moved, because it came from no queue or its
// destination does not exist, is not moved, and the reason is returned.
func (s *Service) redrive(ctx context.Context, tx *sql.Tx, destination string, m *messageRow,
	now time.Time,
) (string, string, error) {
	arn := destination
	if arn == "" {
		arn = m.msg.DeadLetterSourceARN
	}
	if arn == "" {
		return "", "Message " + m.msg.ID + " has no source queue to move it back to: " +
			"it was not moved to the dead-letter queue by a redrive policy.", nil
	}
	missing := "The destination queue " + arn + " does not exist."
	name, ok := s.settings.NameOfARN(arn)
	if !ok {
		return "", missing, nil
	}
	dst, err := loadQueue(ctx, tx, name)
	if errors.Is(err, contracts.ErrQueueNotFound) {
		return "", missing, nil
	}
	if err != nil {
		return "", "", err
	}
	messageID, err := newMessageID()
	if err != nil {
		return "", "", err
	}
	seq := ""
	if dst.fifo {
		seq = sequenceNumber(dst.nextSeq)
		if err = exec(ctx, tx, `UPDATE queues SET next_sequence = ? WHERE id = ?`,
			dst.nextSeq+1, dst.id); err != nil {
			return "", "", err
		}
	}
	if err = exec(ctx, tx,
		`INSERT INTO messages (queue_id, message_id, body, md5_body, attributes,
		   md5_attributes, system_attributes, md5_system, sender_id, sent_at, enqueued_at,
		   visible_at, group_id, dedup_id, sequence_number)
		 SELECT ?, ?, body, md5_body, attributes, md5_attributes, system_attributes,
		   md5_system, sender_id, sent_at, ?, ?, group_id, dedup_id, ?
		 FROM messages WHERE id = ?`,
		dst.id, messageID, stamp(now), stamp(now), seq, m.id); err != nil {
		return "", "", err
	}
	return name, "", exec(ctx, tx, `DELETE FROM messages WHERE id = ?`, m.id)
}
