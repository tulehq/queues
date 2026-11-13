package sqlimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"tule.dev/queues/internal/contracts"
)

// ReceiveMessages receives up to in.Max messages. When none is available it
// waits up to the wait time, the request's or the queue's, for one: it wakes
// when a message is sent or made visible, and when the next delayed or
// in-flight message is due on the service clock, then looks again. It
// returns as soon as it has one message, as SQS's long polling does. A wait
// a client gives up on ends with the context's error.
func (s *Service) ReceiveMessages(ctx context.Context, name string, in contracts.ReceiveInput) (
	[]*contracts.Message, error,
) {
	var deadline *time.Timer
	defer func() {
		if deadline != nil {
			deadline.Stop()
		}
	}()
	for {
		woken := s.signals.wait(name)
		l, err := s.look(ctx, name, in)
		if err != nil || l.final() || s.signals.isStopped() {
			return l.msgs, err
		}
		if deadline == nil {
			deadline = time.NewTimer(l.wait)
		}
		switch s.sleep(ctx, woken, deadline.C, l.next) {
		case wokeDeadline:
			return nil, nil
		case wokeCanceled:
			return nil, fmt.Errorf("receive from %s: %w", name, ctx.Err())
		case wokeToLook:
		}
	}
}

// What ended a sleep.
const (
	wokeToLook = iota + 1
	wokeDeadline
	wokeCanceled
)

// sleep waits for a signal, the deadline, ctx to end, or the time next,
// when it is set, on the service clock, and returns which came first.
func (s *Service) sleep(ctx context.Context, woken <-chan struct{}, deadline <-chan time.Time,
	next time.Time,
) int {
	var due <-chan time.Time
	if !next.IsZero() {
		t := time.NewTimer(max(next.Sub(s.now()), 0))
		defer t.Stop()
		due = t.C
	}
	select {
	case <-woken:
	case <-due:
	case <-deadline:
		return wokeDeadline
	case <-ctx.Done():
		return wokeCanceled
	}
	return wokeToLook
}

// looked is what one look for messages found: the messages it received,
// the wait the request has (0 for a short poll), and when the queue's next
// delayed or in-flight message is due, or the zero time when none is.
type looked struct {
	next time.Time
	msgs []*contracts.Message
	wait time.Duration
}

// final reports whether a look ends the receive: it found messages, or the
// receive is a short poll.
func (l *looked) final() bool {
	return len(l.msgs) > 0 || l.wait == 0
}

// look is one look for messages, in one transaction. The dead-letter
// queues it moved messages to are signaled once it commits.
func (s *Service) look(ctx context.Context, name string, in contracts.ReceiveInput) (
	looked, error,
) {
	var (
		l     looked
		moved []string
	)
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		l, moved, err = s.receive(ctx, tx, name, in)
		return err
	})
	if err == nil && len(moved) > 0 {
		s.signals.signal(moved...)
	}
	return l, err
}

// receive is look's transaction. It returns what it found and the names
// of the dead-letter queues it moved messages to.
func (s *Service) receive(ctx context.Context, tx *sql.Tx, name string,
	in contracts.ReceiveInput,
) (looked, []string, error) {
	now := s.now()
	q, set, err := openQueue(ctx, tx, name, now)
	if err != nil {
		return looked{}, nil, err
	}
	l := looked{wait: seconds(in.WaitSeconds, set.Wait)}
	visibility := seconds(in.VisibilityTimeout, set.VisibilityTimeout)
	attempt := ""
	if q.fifo {
		attempt = in.AttemptID
	}
	if attempt != "" {
		if l.msgs, err = s.retryAttempt(ctx, tx, q, attempt, now); err != nil || l.msgs != nil {
			return l, nil, err
		}
	}
	t := &taking{q: q, set: set, visibility: visibility, now: now}
	if err = s.take(ctx, tx, t, in.Max); err != nil {
		return l, nil, err
	}
	l.msgs = t.out
	return l, t.moved, finish(ctx, tx, t, attempt, &l)
}

// finish records a FIFO receive's attempt ID with the messages it took, or,
// for a long poll that took none, sets when the queue's next message is due.
func finish(ctx context.Context, tx *sql.Tx, t *taking, attempt string, l *looked) error {
	var err error
	switch {
	case len(t.out) > 0 && attempt != "":
		err = saveAttempt(ctx, tx, t.q, attempt, t.visibility, t.receipts, t.now)
	case len(t.out) == 0 && l.wait > 0:
		l.next, err = nextDue(ctx, tx, t.q, t.now)
	}
	return err
}

// seconds returns v seconds, or def when v is nil.
func seconds(v *int, def time.Duration) time.Duration {
	if v == nil {
		return def
	}
	return time.Duration(*v) * time.Second
}

// nextDue returns when the queue's next delayed or in-flight message is
// due, or the zero time when none is.
func nextDue(ctx context.Context, tx *sql.Tx, q *queueRow, now time.Time) (time.Time, error) {
	var due sql.NullInt64
	if _, err := scanOne(ctx, tx, []any{&due},
		`SELECT MIN(visible_at) FROM messages WHERE queue_id = ? AND visible_at > ?`,
		q.id, stamp(now)); err != nil || !due.Valid {
		return time.Time{}, err
	}
	return unstamp(due.Int64), nil
}

// taking is one receive's progress through a queue.
type taking struct {
	now        time.Time
	q          *queueRow
	out        []*contracts.Message
	receipts   []attemptReceipt
	moved      []string
	set        contracts.Settings
	visibility time.Duration
}

// take receives up to n messages.
func (s *Service) take(ctx context.Context, tx *sql.Tx, t *taking, n int) error {
	if t.q.fifo {
		return s.takeFIFO(ctx, tx, t, n)
	}
	return s.takeStandard(ctx, tx, t, n)
}

// takeStandard receives up to n visible messages of a standard queue, in
// the order they became visible: a message never received in the order it
// was sent. Each query reads only the messages it may take, through the
// visibility index, so the cost does not grow with what else the queue
// holds. A message taken with a visibility timeout of 0 stays visible, so
// those taken already are left out of the next query.
func (s *Service) takeStandard(ctx context.Context, tx *sql.Tx, t *taking, n int) error {
	for len(t.out) < n {
		query := `SELECT ` + messageColumns + ` FROM messages
			WHERE queue_id = ? AND visible_at <= ?`
		args := make([]any, 0, 3+len(t.receipts))
		args = append(args, t.q.id, stamp(t.now))
		for _, r := range t.receipts {
			query += ` AND id != ?`
			args = append(args, r.Row)
		}
		rows, err := queryAll(ctx, tx, scanMessage, query+` ORDER BY visible_at, id LIMIT ?`,
			append(args, n-len(t.out))...)
		if err != nil || len(rows) == 0 {
			return err
		}
		for _, m := range rows {
			if err = s.takeOne(ctx, tx, t, m); err != nil {
				return err
			}
		}
	}
	return nil
}

// fifoHeads selects, in queue order, up to n groups of a FIFO queue that
// may be received from: groups with no message in flight, wherever in the
// queue it is, whose first message is visible. It walks the distinct groups
// through the group index, one seek per group, rather than the messages,
// so a group with a long backlog behind a message in flight costs one
// seek, not a read of the backlog.
const fifoHeads = `
WITH RECURSIVE grp(g) AS (
	SELECT (SELECT group_id FROM messages WHERE queue_id = ?1 ORDER BY group_id LIMIT 1)
	UNION ALL
	SELECT (SELECT group_id FROM messages
	        WHERE queue_id = ?1 AND group_id > grp.g ORDER BY group_id LIMIT 1)
	FROM grp WHERE grp.g IS NOT NULL
),
heads AS (
	SELECT g, (SELECT id FROM messages WHERE queue_id = ?1 AND group_id = grp.g
	           ORDER BY id LIMIT 1) AS head
	FROM grp
	WHERE g IS NOT NULL AND NOT EXISTS (
		SELECT 1 FROM messages x
		WHERE x.queue_id = ?1 AND x.group_id = grp.g AND x.visible_at > ?2
		  AND x.receive_count > 0)
)
SELECT h.g FROM heads h JOIN messages m ON m.id = h.head
WHERE m.visible_at <= ?2
ORDER BY h.head LIMIT ?3`

// takeFIFO receives up to n messages of a FIFO queue. A group is held while
// any of its messages is in flight, so only the groups fifoHeads selects
// are read, and each from its first message, in order, as many as it has
// visible up to n, as SQS returns as many of one group as it can. A message
// that is not visible stops its group, so the order is kept.
func (s *Service) takeFIFO(ctx context.Context, tx *sql.Tx, t *taking, n int) error {
	groups, err := queryAll(ctx, tx, scanString, fifoHeads, t.q.id, stamp(t.now), n)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if len(t.out) == n {
			return nil
		}
		rows, qErr := queryAll(ctx, tx, scanMessage,
			`SELECT `+messageColumns+` FROM messages WHERE queue_id = ? AND group_id = ?
			 ORDER BY id LIMIT ?`, t.q.id, g, n-len(t.out))
		if qErr != nil {
			return qErr
		}
		for _, m := range rows {
			if m.visibleAt > stamp(t.now) {
				break
			}
			if err = s.takeOne(ctx, tx, t, m); err != nil {
				return err
			}
		}
	}
	return nil
}

// takeOne receives the message m, or moves it to the dead-letter queue
// instead when its receive count has reached maxReceiveCount.
func (s *Service) takeOne(ctx context.Context, tx *sql.Tx, t *taking, m *messageRow) error {
	if t.set.Redrive != nil && m.msg.ReceiveCount >= t.set.Redrive.MaxReceiveCount {
		dlq, err := s.moveToDeadLetter(ctx, tx, t.q, m, t.set.Redrive, t.now)
		if err != nil || dlq != "" {
			if dlq != "" {
				t.moved = append(t.moved, dlq)
			}
			return err
		}
	}
	return markReceived(ctx, tx, t, m)
}

// markReceived receives the message m under a new receipt: in flight for
// the visibility timeout, its receive count one more.
func markReceived(ctx context.Context, tx *sql.Tx, t *taking, m *messageRow) error {
	nonce, err := newNonce()
	if err != nil {
		return err
	}
	m.receipt = nonce
	m.receivedAt = stamp(t.now)
	m.visibleAt = stamp(t.now.Add(t.visibility))
	m.msg.ReceiveCount++
	if m.msg.FirstReceivedAt.IsZero() {
		m.msg.FirstReceivedAt = unstamp(stamp(t.now))
	}
	if err = exec(ctx, tx,
		`UPDATE messages SET receipt = ?, received_at = ?, visible_at = ?,
		   receive_count = ?, first_received_at = ? WHERE id = ?`,
		m.receipt, m.receivedAt, m.visibleAt, m.msg.ReceiveCount,
		stamp(m.msg.FirstReceivedAt), m.id); err != nil {
		return err
	}
	m.msg.VisibleAt = unstamp(m.visibleAt)
	m.msg.State = contracts.StateInFlight
	m.msg.ReceiptHandle = receipt{queueID: t.q.id, rowID: m.id, nonce: nonce}.encode()
	t.out = append(t.out, &m.msg)
	t.receipts = append(t.receipts, attemptReceipt{Row: m.id, Nonce: nonce})
	return nil
}

// moveToDeadLetter moves a message whose receive count has reached its
// queue's maxReceiveCount to the dead-letter queue, as SQS moves one the
// receive would take past it. It returns the dead-letter queue's name, or ""
// when the queue no longer exists and the message stays.
//
// The message goes to the back of the dead-letter queue, visible, with its
// receive count and with the source queue's ARN as its
// DeadLetterQueueSourceArn. A standard queue's message keeps its enqueue
// time, so its retention still counts from when it was first sent; a FIFO
// queue's starts again, as the SQS developer guide describes both. A FIFO
// message takes the dead-letter queue's next sequence number.
func (s *Service) moveToDeadLetter(ctx context.Context, tx *sql.Tx, q *queueRow, m *messageRow,
	p *contracts.RedrivePolicy, now time.Time,
) (string, error) {
	name, ok := s.settings.NameOfARN(p.DeadLetterTargetArn)
	if !ok {
		return "", nil
	}
	dlq, err := loadQueue(ctx, tx, name)
	if err != nil {
		if errors.Is(err, contracts.ErrQueueNotFound) {
			return "", nil
		}
		return "", err
	}
	enqueuedAt := m.enqueuedAt
	seq := m.msg.SequenceNumber
	if q.fifo {
		enqueuedAt = stamp(now)
		seq = sequenceNumber(dlq.nextSeq)
		if err = exec(ctx, tx, `UPDATE queues SET next_sequence = ? WHERE id = ?`,
			dlq.nextSeq+1, dlq.id); err != nil {
			return "", err
		}
	}
	if err = exec(ctx, tx,
		`INSERT INTO messages (queue_id, message_id, body, md5_body, attributes,
		   md5_attributes, system_attributes, md5_system, sender_id, sent_at, enqueued_at,
		   visible_at, receive_count, first_received_at, group_id, dedup_id,
		   sequence_number, dlq_source_arn)
		 SELECT ?, message_id, body, md5_body, attributes, md5_attributes,
		   system_attributes, md5_system, sender_id, sent_at, ?, ?, receive_count,
		   first_received_at, group_id, dedup_id, ?, ?
		 FROM messages WHERE id = ?`,
		dlq.id, enqueuedAt, stamp(now), seq, s.settings.ARN(q.name), m.id); err != nil {
		return "", err
	}
	return name, exec(ctx, tx, `DELETE FROM messages WHERE id = ?`, m.id)
}

// attemptReceipt is one message a receive attempt returned.
type attemptReceipt struct {
	Nonce string `json:"nonce"`
	Row   int64  `json:"row"`
}

func saveAttempt(ctx context.Context, tx *sql.Tx, q *queueRow, attemptID string,
	visibility time.Duration, receipts []attemptReceipt, now time.Time,
) error {
	raw, err := json.Marshal(receipts)
	if err != nil {
		return fmt.Errorf("encode receive attempt: %w", err)
	}
	return exec(ctx, tx,
		`INSERT INTO receive_attempts (queue_id, attempt_id, expires_at, visibility, receipts)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (queue_id, attempt_id) DO UPDATE SET expires_at = excluded.expires_at,
		   visibility = excluded.visibility, receipts = excluded.receipts`,
		q.id, attemptID, stamp(now.Add(contracts.ReceiveAttemptWindow)),
		visibility.Milliseconds(), string(raw))
}

// retryAttempt answers a FIFO receive that repeats a ReceiveRequestAttemptId
// of the last ReceiveAttemptWindow: when none of the messages that attempt
// returned has been deleted or received again since, it returns them again
// with the same receipt handles, their visibility timeouts started over. It
// returns nil when the attempt is unknown or its messages changed, and the
// receive goes ahead as a new one.
func (s *Service) retryAttempt(ctx context.Context, tx *sql.Tx, q *queueRow, attemptID string,
	now time.Time,
) ([]*contracts.Message, error) {
	receipts, visibility, err := loadAttempt(ctx, tx, q, attemptID, now)
	if err != nil || receipts == nil {
		return nil, err
	}
	visibleAt := stamp(now) + visibility
	out := make([]*contracts.Message, 0, len(receipts))
	for _, r := range receipts {
		rows, qErr := queryAll(ctx, tx, scanMessage,
			`SELECT `+messageColumns+` FROM messages WHERE id = ? AND queue_id = ?`, r.Row, q.id)
		if qErr != nil || len(rows) == 0 || rows[0].receipt != r.Nonce {
			return nil, qErr
		}
		m := rows[0]
		m.visibleAt = min(visibleAt, m.receivedAt+contracts.MaxVisibility.Milliseconds())
		m.msg.VisibleAt = unstamp(m.visibleAt)
		m.msg.State = contracts.StateInFlight
		m.msg.ReceiptHandle = receipt{queueID: q.id, rowID: m.id, nonce: r.Nonce}.encode()
		out = append(out, &m.msg)
	}
	for _, r := range receipts {
		// The receive time stays the first receive's, and the timeout ends
		// no later than MaxVisibility after it, so a retry does not extend
		// the 12 hours a receive may keep a message in flight.
		if err = exec(ctx, tx,
			`UPDATE messages SET visible_at = MIN(?, received_at + ?) WHERE id = ?`,
			visibleAt, contracts.MaxVisibility.Milliseconds(), r.Row); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// loadAttempt returns the messages and the visibility timeout, in
// milliseconds, of the queue's receive attempt, or nil when there is none
// within ReceiveAttemptWindow.
func loadAttempt(ctx context.Context, tx *sql.Tx, q *queueRow, attemptID string,
	now time.Time,
) ([]attemptReceipt, int64, error) {
	if err := exec(ctx, tx, `DELETE FROM receive_attempts WHERE expires_at <= ?`,
		stamp(now)); err != nil {
		return nil, 0, err
	}
	var (
		raw        string
		visibility int64
	)
	found, err := scanOne(ctx, tx, []any{&raw, &visibility},
		`SELECT receipts, visibility FROM receive_attempts
		 WHERE queue_id = ? AND attempt_id = ?`, q.id, attemptID)
	if err != nil || !found {
		return nil, 0, err
	}
	var receipts []attemptReceipt
	if err = json.Unmarshal([]byte(raw), &receipts); err != nil {
		return nil, 0, fmt.Errorf("decode receive attempt: %w", err)
	}
	return receipts, visibility, nil
}
