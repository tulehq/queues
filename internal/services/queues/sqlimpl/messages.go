package sqlimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"tule.dev/queues/internal/contracts"
)

// messageRow is a message as the store keeps it.
type messageRow struct {
	receipt    string
	msg        contracts.Message
	id         int64
	queueID    int64
	enqueuedAt int64
	visibleAt  int64
	receivedAt int64
}

const messageColumns = `id, queue_id, message_id, body, md5_body, attributes,
	md5_attributes, system_attributes, md5_system, sender_id, sent_at, enqueued_at,
	visible_at, receive_count, first_received_at, received_at, receipt, group_id,
	dedup_id, sequence_number, dlq_source_arn`

func scanMessage(r scanner) (*messageRow, error) {
	var (
		m                    messageRow
		attrs, system        string
		sentAt, firstReceive int64
	)
	if err := r.Scan(&m.id, &m.queueID, &m.msg.ID, &m.msg.Body, &m.msg.MD5OfBody, &attrs,
		&m.msg.MD5OfAttributes, &system, &m.msg.MD5OfSystem, &m.msg.SenderID, &sentAt,
		&m.enqueuedAt, &m.visibleAt, &m.msg.ReceiveCount, &firstReceive, &m.receivedAt,
		&m.receipt, &m.msg.GroupID, &m.msg.DeduplicationID, &m.msg.SequenceNumber,
		&m.msg.DeadLetterSourceARN); err != nil {
		return nil, err //nolint:wrapcheck // queryAll wraps it
	}
	if err := json.Unmarshal([]byte(attrs), &m.msg.Attributes); err != nil {
		return nil, fmt.Errorf("stored attributes of message %s: %w", m.msg.ID, err)
	}
	if err := json.Unmarshal([]byte(system), &m.msg.SystemAttributes); err != nil {
		return nil, fmt.Errorf("stored system attributes of message %s: %w", m.msg.ID, err)
	}
	m.msg.SentAt = unstamp(sentAt)
	m.msg.FirstReceivedAt = unstampOrZero(firstReceive)
	m.msg.VisibleAt = unstamp(m.visibleAt)
	return &m, nil
}

// stateAt sets the message's state at now.
func (m *messageRow) stateAt(now time.Time) {
	switch {
	case m.visibleAt <= stamp(now):
		m.msg.State = contracts.StateVisible
	case m.msg.ReceiveCount > 0:
		m.msg.State = contracts.StateInFlight
	default:
		m.msg.State = contracts.StateDelayed
	}
}

func encodeAttributes(attrs map[string]contracts.MessageAttribute) (string, error) {
	if attrs == nil {
		attrs = map[string]contracts.MessageAttribute{}
	}
	raw, err := json.Marshal(attrs)
	if err != nil {
		return "", fmt.Errorf("encode message attributes: %w", err)
	}
	return string(raw), nil
}

// SendMessages sends msgs to the queue in order, in one transaction. On a
// FIFO queue each message takes the queue's next sequence number, and one
// whose deduplication ID the queue saw within DeduplicationInterval, in the
// queue or in its group as DeduplicationScope says, is not stored: its
// result is the message it duplicates.
func (s *Service) SendMessages(ctx context.Context, name string, msgs []contracts.SendInput) (
	[]contracts.SendResult, error,
) {
	out := make([]contracts.SendResult, 0, len(msgs))
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		set, err := q.settings()
		if err != nil {
			return err
		}
		now := s.now()
		if err = exec(ctx, tx, `DELETE FROM deduplication WHERE expires_at <= ?`,
			stamp(now)); err != nil {
			return err
		}
		for i := range msgs {
			res, sErr := s.send(ctx, tx, q, set, &msgs[i], now)
			if sErr != nil {
				return sErr
			}
			out = append(out, res)
		}
		if q.fifo {
			return exec(ctx, tx, `UPDATE queues SET next_sequence = ? WHERE id = ?`,
				q.nextSeq, q.id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.signals.signal(name)
	return out, nil
}

func (s *Service) send(ctx context.Context, tx *sql.Tx, q *queueRow, set contracts.Settings,
	m *contracts.SendInput, now time.Time,
) (contracts.SendResult, error) {
	var res contracts.SendResult
	group := ""
	if q.fifo {
		if set.DeduplicationScope == contracts.DeduplicationScopeMessageGroup {
			group = m.GroupID
		}
		dup, found, err := duplicate(ctx, tx, q, group, m.DeduplicationID)
		if err != nil || found {
			return dup, err
		}
		res.SequenceNumber = sequenceNumber(q.nextSeq)
		q.nextSeq++
	}

	id, err := insertMessage(ctx, tx, q, set, m, res.SequenceNumber, now)
	if err != nil || !q.fifo {
		return contracts.SendResult{MessageID: id, SequenceNumber: res.SequenceNumber}, err
	}
	res.MessageID = id
	return res, exec(ctx, tx,
		`INSERT INTO deduplication (queue_id, group_id, dedup_id, message_id,
		   sequence_number, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		q.id, group, m.DeduplicationID, id, res.SequenceNumber,
		stamp(now.Add(contracts.DeduplicationInterval)))
}

// insertMessage stores a new message, delayed by its own delay or the
// queue's, and returns its ID.
func insertMessage(ctx context.Context, tx *sql.Tx, q *queueRow, set contracts.Settings,
	m *contracts.SendInput, seq string, now time.Time,
) (string, error) {
	id, err := newMessageID()
	if err != nil {
		return "", err
	}
	delay := seconds(m.DelaySeconds, set.Delay)
	attrs, err := encodeAttributes(m.Attributes)
	if err != nil {
		return "", err
	}
	system, err := encodeAttributes(m.SystemAttributes)
	if err != nil {
		return "", err
	}
	return id, exec(ctx, tx,
		`INSERT INTO messages (queue_id, message_id, body, md5_body, attributes,
		   md5_attributes, system_attributes, md5_system, sender_id, sent_at, enqueued_at,
		   visible_at, group_id, dedup_id, sequence_number)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		q.id, id, m.Body, m.MD5OfBody, attrs, m.MD5OfAttributes, system,
		m.MD5OfSystemAttributes, m.SenderID, stamp(now), stamp(now), stamp(now.Add(delay)),
		m.GroupID, m.DeduplicationID, seq)
}

// duplicate returns the message a FIFO queue's deduplication ID, in the
// group or the queue, stands for within DeduplicationInterval, reporting
// false when it stands for none.
func duplicate(ctx context.Context, tx *sql.Tx, q *queueRow, group, id string) (
	contracts.SendResult, bool, error,
) {
	var res contracts.SendResult
	found, err := scanOne(ctx, tx, []any{&res.MessageID, &res.SequenceNumber},
		`SELECT message_id, sequence_number FROM deduplication
		 WHERE queue_id = ? AND group_id = ? AND dedup_id = ?`,
		q.id, group, id)
	return res, found, err
}

// receivedMessage returns the message of a receipt handle of the queue, or
// nil when the handle names no message of the queue that exists: one this
// store did not issue for the queue, or of a message that is gone. The
// receipt is the zero value for a handle this store did not issue.
func receivedMessage(ctx context.Context, tx *sql.Tx, q *queueRow, handle string) (
	*messageRow, receipt, error,
) {
	rc, ok := parseReceipt(handle)
	if !ok || rc.queueID != q.id {
		return nil, rc, nil
	}
	rows, err := queryAll(ctx, tx, scanMessage,
		`SELECT `+messageColumns+` FROM messages WHERE id = ? AND queue_id = ?`,
		rc.rowID, q.id)
	if err != nil || len(rows) == 0 {
		return nil, rc, err
	}
	return rows[0], rc, nil
}

// DeleteMessages deletes the messages of receipt handles. A handle of a
// message already gone, or of an earlier receive of a message received since,
// succeeds and deletes nothing, as SQS's DeleteMessage documents an old
// handle: the request succeeds but the message might not be deleted. A
// handle this store did not issue for the queue fails.
func (s *Service) DeleteMessages(ctx context.Context, name string, handles []string) (
	[]error, error,
) {
	out := make([]error, len(handles))
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, _, err := openQueue(ctx, tx, name, s.now())
		if err != nil {
			return err
		}
		for i, h := range handles {
			if out[i], err = deleteOne(ctx, tx, q, h); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A FIFO group whose message in flight is gone may have the next ready.
	s.signals.signal(name)
	return out, nil
}

// deleteOne deletes the message of one receipt handle, returning the
// entry's error or, second, a failure of the store.
//
//nolint:revive // the entry's error, then the store's
func deleteOne(ctx context.Context, tx *sql.Tx, q *queueRow, handle string) (error, error) {
	if rc, ok := parseReceipt(handle); !ok || rc.queueID != q.id {
		return &contracts.ReceiptError{Handle: handle}, nil
	}
	m, rc, err := receivedMessage(ctx, tx, q, handle)
	if err != nil || m == nil || m.receipt != rc.nonce {
		return nil, err
	}
	return nil, exec(ctx, tx, `DELETE FROM messages WHERE id = ?`, m.id)
}

// ChangeVisibility changes the visibility timeouts of received messages,
// counted from now. A handle that names no message of the queue fails with a
// *contracts.ReceiptError; one whose receive is no longer in flight, because
// its timeout ended or the message was received again, with
// contracts.ErrMessageNotInflight; and a timeout that would keep the message
// in flight past MaxVisibility from its receive with a
// *contracts.InvalidError.
func (s *Service) ChangeVisibility(ctx context.Context, name string,
	changes []contracts.VisibilityChange,
) ([]error, error) {
	out := make([]error, len(changes))
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		now := s.now()
		q, _, err := openQueue(ctx, tx, name, now)
		if err != nil {
			return err
		}
		for i, c := range changes {
			if out[i], err = changeOne(ctx, tx, q, c, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.signals.signal(name)
	return out, nil
}

// changeOne changes one message's visibility, returning the entry's error
// or, second, a failure of the store.
func changeOne(ctx context.Context, tx *sql.Tx, q *queueRow, c contracts.VisibilityChange,
	now time.Time,
) (error, error) { //nolint:revive // the entry's error, then the store's
	m, rc, err := receivedMessage(ctx, tx, q, c.ReceiptHandle)
	switch {
	case err != nil:
		return nil, err
	case m == nil:
		return &contracts.ReceiptError{Handle: c.ReceiptHandle}, nil
	case m.receipt != rc.nonce || m.visibleAt <= stamp(now):
		return contracts.ErrMessageNotInflight, nil
	}
	visibleAt := now.Add(time.Duration(c.Timeout) * time.Second)
	if visibleAt.Sub(unstamp(m.receivedAt)) > contracts.MaxVisibility {
		return &contracts.InvalidError{
			Field: "VisibilityTimeout", Value: strconv.Itoa(c.Timeout),
			Reason: "Total VisibilityTimeout for the message is beyond the limit [43200 seconds]",
		}, nil
	}
	return nil, exec(ctx, tx, `UPDATE messages SET visible_at = ? WHERE id = ?`,
		stamp(visibleAt), m.id)
}
