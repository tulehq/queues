package sqlimpl

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"tule.dev/queues/internal/contracts"
)

// The inspection operations, which read and reset the store without
// changing what SQS clients see of a message: no receive count, visibility
// or receipt changes.

// stateFilters are the SQL conditions of each message state, with the time
// they compare against as their one argument.
var stateFilters = map[string]string{
	contracts.StateVisible:  ` AND visible_at <= ?`,
	contracts.StateInFlight: ` AND visible_at > ? AND receive_count > 0`,
	contracts.StateDelayed:  ` AND visible_at > ? AND receive_count = 0`,
}

// ListMessages lists the queue's messages in queue order, those in the
// filter's state and group when it names them. Messages past their
// retention period are deleted first, as a receive would.
func (s *Service) ListMessages(ctx context.Context, name string, f contracts.MessageFilter,
	page contracts.PageInput,
) (*contracts.Page[*contracts.Message], error) {
	var out *contracts.Page[*contracts.Message]
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		now := s.now()
		q, _, err := openQueue(ctx, tx, name, now)
		if err != nil {
			return err
		}
		scope := name + "|" + f.State + "|" + f.Group
		after, err := decodeRowToken(page.NextToken, scope)
		if err != nil {
			return err
		}
		query, args := messageQuery(q, f, after, now)
		rows, err := queryAll(ctx, tx, scanMessage, query, append(args, page.MaxResults+1)...)
		if err != nil {
			return err
		}
		out = &contracts.Page[*contracts.Message]{
			Items: make([]*contracts.Message, 0, min(len(rows), page.MaxResults)),
		}
		for i, m := range rows {
			if i == page.MaxResults {
				out.NextToken = encodeToken(listMessages, scope,
					strconv.FormatInt(rows[i-1].id, 10))
				break
			}
			m.stateAt(now)
			out.Items = append(out.Items, &m.msg)
		}
		return nil
	})
	return out, err
}

// decodeRowToken returns the message row a listing's token resumes after,
// or 0 for an empty token.
func decodeRowToken(token, scope string) (int64, error) {
	cursor, err := decodeToken(token, listMessages, scope)
	if err != nil || cursor == "" {
		return 0, err
	}
	after, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil || after < 0 {
		return 0, contracts.ErrInvalidToken
	}
	return after, nil
}

// messageQuery returns the query of a page of the queue's messages after
// the row after, in the filter's state and group, and its arguments but the
// page size, which it ends with a placeholder for.
func messageQuery(q *queueRow, f contracts.MessageFilter, after int64, now time.Time) (
	string, []any,
) {
	query := `SELECT ` + messageColumns + ` FROM messages WHERE queue_id = ? AND id > ?`
	args := []any{q.id, after}
	if f.State != "" {
		query += stateFilters[f.State]
		args = append(args, stamp(now))
	}
	if f.Group != "" {
		query += ` AND group_id = ?`
		args = append(args, f.Group)
	}
	return query + ` ORDER BY id LIMIT ?`, args
}

// ListAllQueues returns every queue with its attributes and counts, in name
// order.
func (s *Service) ListAllQueues(ctx context.Context) ([]*contracts.Queue, error) {
	var out []*contracts.Queue
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		rows, err := queryAll(ctx, tx, scanQueue,
			`SELECT `+queueColumns+` FROM queues ORDER BY name`)
		if err != nil {
			return err
		}
		now := s.now()
		out = make([]*contracts.Queue, 0, len(rows))
		for _, q := range rows {
			d, dErr := s.describe(ctx, tx, q, now)
			if dErr != nil {
				return dErr
			}
			out = append(out, d)
		}
		return nil
	})
	return out, err
}

// PurgeAll deletes every message of every queue, with the FIFO
// deduplication and receive-attempt records, so the queues start over as if
// new. It neither counts as a PurgeQueue nor is held to PurgeWindow.
func (s *Service) PurgeAll(ctx context.Context) error {
	var names []string
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		if names, err = queryAll(ctx, tx, scanString, `SELECT name FROM queues`); err != nil {
			return err
		}
		for _, table := range []string{"messages", "deduplication", "receive_attempts"} {
			if err = exec(ctx, tx, `DELETE FROM `+table); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		s.signals.signal(names...)
	}
	return err
}

// DeleteAll deletes every queue. Their names can be created again at once:
// it is not held to QueueDeleteWindow, and it forgets the names DeleteQueue
// holds.
func (s *Service) DeleteAll(ctx context.Context) error {
	var names []string
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		var err error
		if names, err = queryAll(ctx, tx, scanString, `SELECT name FROM queues`); err != nil {
			return err
		}
		for _, table := range []string{
			"messages", "queue_tags", "deduplication",
			"receive_attempts", "move_tasks", "deleted_queues", "queues",
		} {
			if err = exec(ctx, tx, `DELETE FROM `+table); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		s.signals.signal(names...)
	}
	return err
}
