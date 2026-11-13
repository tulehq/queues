package sqlimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"tule.dev/queues/internal/contracts"
)

// queueRow is a queue as the store keeps it.
type queueRow struct {
	attrs      map[string]string
	name       string
	id         int64
	createdAt  int64
	modifiedAt int64
	purgedAt   int64
	nextSeq    int64
	fifo       bool
}

const queueColumns = `id, name, fifo, attributes, created_at, modified_at,
	COALESCE(purged_at, 0), next_sequence`

func scanQueue(r scanner) (*queueRow, error) {
	var (
		q     queueRow
		attrs string
	)
	if err := r.Scan(&q.id, &q.name, &q.fifo, &attrs, &q.createdAt, &q.modifiedAt,
		&q.purgedAt, &q.nextSeq); err != nil {
		return nil, err //nolint:wrapcheck // the callers wrap it
	}
	if err := json.Unmarshal([]byte(attrs), &q.attrs); err != nil {
		return nil, fmt.Errorf("stored attributes of queue %s: %w", q.name, err)
	}
	return &q, nil
}

// loadQueue returns the queue name, or contracts.ErrQueueNotFound.
func loadQueue(ctx context.Context, tx *sql.Tx, name string) (*queueRow, error) {
	rows, err := queryAll(ctx, tx, scanQueue,
		`SELECT `+queueColumns+` FROM queues WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, contracts.ErrQueueNotFound
	}
	return rows[0], nil
}

func (q *queueRow) settings() (contracts.Settings, error) {
	return contracts.SettingsOf(q.attrs) //nolint:wrapcheck // it names the attribute
}

// expire deletes the queue's messages older than its retention period, as
// SQS deletes them.
func expire(ctx context.Context, tx *sql.Tx, q *queueRow, set contracts.Settings,
	now time.Time,
) error {
	return exec(ctx, tx, `DELETE FROM messages WHERE queue_id = ? AND enqueued_at <= ?`,
		q.id, stamp(now.Add(-set.Retention)))
}

// openQueue loads the queue name and its settings, and deletes its messages
// past their retention period at now, as every operation on its messages
// does first.
func openQueue(ctx context.Context, tx *sql.Tx, name string, now time.Time) (
	*queueRow, contracts.Settings, error,
) {
	q, err := loadQueue(ctx, tx, name)
	if err != nil {
		return nil, contracts.Settings{}, err
	}
	set, err := q.settings()
	if err != nil {
		return nil, contracts.Settings{}, err
	}
	return q, set, expire(ctx, tx, q, set, now)
}

// counts returns the numbers of the queue's messages in each state.
func counts(ctx context.Context, tx *sql.Tx, q *queueRow, now time.Time) (contracts.Counts,
	error,
) {
	var c contracts.Counts
	_, err := scanOne(ctx, tx, []any{&c.Visible, &c.InFlight, &c.Delayed},
		`SELECT COALESCE(SUM(visible_at <= ?), 0),
		        COALESCE(SUM(visible_at > ? AND receive_count > 0), 0),
		        COALESCE(SUM(visible_at > ? AND receive_count = 0), 0)
		 FROM messages WHERE queue_id = ?`,
		stamp(now), stamp(now), stamp(now), q.id)
	return c, err
}

// describe returns the queue as the contract reports it, its expired
// messages deleted first so the counts leave them out.
func (s *Service) describe(ctx context.Context, tx *sql.Tx, q *queueRow, now time.Time) (
	*contracts.Queue, error,
) {
	set, err := q.settings()
	if err != nil {
		return nil, err
	}
	if err = expire(ctx, tx, q, set, now); err != nil {
		return nil, err
	}
	c, err := counts(ctx, tx, q, now)
	if err != nil {
		return nil, err
	}
	return &contracts.Queue{
		Name:       q.name,
		FIFO:       q.fifo,
		Attributes: maps.Clone(q.attrs),
		CreatedAt:  unstamp(q.createdAt),
		ModifiedAt: unstamp(q.modifiedAt),
		Counts:     c,
	}, nil
}

// CreateQueue creates a queue, or returns the queue of the name when every
// attribute the request sets has the value the queue has. Tags are not
// compared.
func (s *Service) CreateQueue(ctx context.Context, in contracts.CreateQueueInput) (
	*contracts.Queue, error,
) {
	var out *contracts.Queue
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		now := s.now()
		existing, err := loadQueue(ctx, tx, in.Name)
		switch {
		case err == nil:
			if !sameAttributes(existing.attrs, in.Attributes) {
				return contracts.ErrQueueExists
			}
			out, err = s.describe(ctx, tx, existing, now)
			return err
		case !errors.Is(err, contracts.ErrQueueNotFound):
			return err
		}
		if err = checkDeletedRecently(ctx, tx, in.Name, now); err != nil {
			return err
		}
		if err = s.checkRedrive(ctx, tx, in.Name, in.FIFO, in.Attributes); err != nil {
			return err
		}
		out, err = insertQueue(ctx, tx, in, now)
		return err
	})
	return out, err
}

// checkDeletedRecently fails with contracts.ErrQueueDeletedRecently when
// DeleteQueue deleted a queue of the name within QueueDeleteWindow.
func checkDeletedRecently(ctx context.Context, tx *sql.Tx, name string, now time.Time) error {
	var deletedAt int64
	found, err := scanOne(ctx, tx, []any{&deletedAt},
		`SELECT deleted_at FROM deleted_queues WHERE name = ?`, name)
	if err != nil {
		return err
	}
	if found && now.Sub(unstamp(deletedAt)) < contracts.QueueDeleteWindow {
		return contracts.ErrQueueDeletedRecently
	}
	return nil
}

// insertQueue stores a new queue, with the defaults of the attributes the
// request leaves out, and its tags.
func insertQueue(ctx context.Context, tx *sql.Tx, in contracts.CreateQueueInput,
	now time.Time,
) (*contracts.Queue, error) {
	attrs := contracts.Defaults(in.FIFO)
	applyAttributes(attrs, in.Attributes)
	raw, err := json.Marshal(attrs)
	if err != nil {
		return nil, fmt.Errorf("encode attributes: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`INSERT INTO queues (name, fifo, attributes, created_at, modified_at)
		 VALUES (?, ?, ?, ?, ?)`,
		in.Name, in.FIFO, string(raw), stamp(now), stamp(now))
	if err != nil {
		return nil, fmt.Errorf("insert queue: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("insert queue: %w", err)
	}
	if err = putTags(ctx, tx, id, in.Tags); err != nil {
		return nil, err
	}
	return &contracts.Queue{
		Name: in.Name, FIFO: in.FIFO, Attributes: attrs,
		CreatedAt: unstamp(stamp(now)), ModifiedAt: unstamp(stamp(now)),
	}, nil
}

// sameAttributes reports whether every attribute of want has its value in
// have, an absent attribute counting as "".
func sameAttributes(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// applyAttributes sets attrs on a queue's attributes, removing those set
// to "".
func applyAttributes(dst, attrs map[string]string) {
	for k, v := range attrs {
		if v == "" {
			delete(dst, k)
			continue
		}
		dst[k] = v
	}
}

// checkRedrive checks a RedrivePolicy the request sets on the queue name:
// its dead-letter queue must exist in this account and region, be of the
// same type, not be the queue itself, and allow the queue as a source.
func (s *Service) checkRedrive(ctx context.Context, tx *sql.Tx, name string, fifo bool,
	attrs map[string]string,
) error {
	v := attrs[contracts.AttrRedrivePolicy]
	if v == "" {
		return nil
	}
	var redrive contracts.RedrivePolicy
	if err := json.Unmarshal([]byte(v), &redrive); err != nil {
		return fmt.Errorf("decode the checked RedrivePolicy: %w", err)
	}
	invalid := func(reason string) error {
		return &contracts.InvalidError{
			Field: contracts.AttrRedrivePolicy, Value: v,
			Reason: reason,
		}
	}
	target, ok := s.settings.NameOfARN(redrive.DeadLetterTargetArn)
	if !ok {
		return invalid("Dead letter target does not exist")
	}
	if target == name {
		return invalid("A queue cannot be its own dead-letter queue")
	}
	dlq, err := loadQueue(ctx, tx, target)
	if errors.Is(err, contracts.ErrQueueNotFound) {
		return invalid("Dead letter target does not exist")
	}
	if err != nil {
		return err
	}
	if dlq.fifo != fifo {
		return invalid("Dead-letter queue must be same type of queue as the source")
	}
	dlqSet, err := dlq.settings()
	if err != nil {
		return err
	}
	if !allowsSource(dlqSet.AllowPolicy, s.settings.ARN(name)) {
		return invalid("The dead-letter queue's RedriveAllowPolicy does not allow " +
			s.settings.ARN(name) + " as a source queue")
	}
	return nil
}

// allowsSource reports whether a dead-letter queue's RedriveAllowPolicy lets
// the queue of the ARN name it: allowAll, the default, or byQueue with it
// among the sources.
func allowsSource(p *contracts.RedriveAllowPolicy, arn string) bool {
	if p == nil || p.RedrivePermission == contracts.RedrivePermissionAllowAll {
		return true
	}
	return p.RedrivePermission == contracts.RedrivePermissionByQueue &&
		slices.Contains(p.SourceQueueArns, arn)
}

// GetQueue returns the queue name with its attributes and counts.
func (s *Service) GetQueue(ctx context.Context, name string) (*contracts.Queue, error) {
	var out *contracts.Queue
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		out, err = s.describe(ctx, tx, q, s.now())
		return err
	})
	return out, err
}

// QueueInfo returns the queue name with its attributes, and zero counts:
// it reads the queue's row alone.
func (s *Service) QueueInfo(ctx context.Context, name string) (*contracts.Queue, error) {
	var out *contracts.Queue
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		out = &contracts.Queue{
			Name: q.name, FIFO: q.fifo, Attributes: q.attrs,
			CreatedAt: unstamp(q.createdAt), ModifiedAt: unstamp(q.modifiedAt),
		}
		return nil
	})
	return out, err
}

// deleteQueueRows deletes the queue's rows in every table.
func deleteQueueRows(ctx context.Context, tx *sql.Tx, queueID int64) error {
	for _, table := range []string{"messages", "queue_tags", "deduplication", "receive_attempts"} {
		if err := exec(ctx, tx, `DELETE FROM `+table+` WHERE queue_id = ?`, queueID); err != nil {
			return err
		}
	}
	return exec(ctx, tx, `DELETE FROM queues WHERE id = ?`, queueID)
}

// DeleteQueue deletes the queue and its messages. Its name cannot be
// created again for QueueDeleteWindow.
func (s *Service) DeleteQueue(ctx context.Context, name string) error {
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		if err = deleteQueueRows(ctx, tx, q.id); err != nil {
			return err
		}
		return exec(ctx, tx,
			`INSERT INTO deleted_queues (name, deleted_at) VALUES (?, ?)
			 ON CONFLICT (name) DO UPDATE SET deleted_at = excluded.deleted_at`,
			name, stamp(s.now()))
	})
	if err == nil {
		s.signals.signal(name)
	}
	return err
}

// ListQueues lists the names of the queues that start with prefix, in
// order. Without MaxResults it returns up to MaxListResults names and no
// token, as SQS does.
func (s *Service) ListQueues(ctx context.Context, prefix string, page contracts.PageInput) (
	*contracts.Page[string], error,
) {
	var out *contracts.Page[string]
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		after, err := decodeToken(page.NextToken, listQueues, prefix)
		if err != nil {
			return err
		}
		size := page.MaxResults
		if size == 0 {
			size = maxListResults
		}
		names, err := queryAll(ctx, tx, scanString,
			`SELECT name FROM queues WHERE substr(name, 1, length(?)) = ? AND name > ?
			 ORDER BY name LIMIT ?`,
			prefix, prefix, after, size+1)
		if err != nil {
			return err
		}
		out = pageOf(names, size, page.MaxResults > 0, listQueues, prefix)
		return nil
	})
	return out, err
}

// maxListResults is how many queues a listing returns without MaxResults.
const maxListResults = 1000

// pageOf makes a page of up to size names from names, which the query
// fetched with one more than size. The page has a token for the next when
// there is one and tokens is set: SQS returns one only when the request set
// MaxResults.
func pageOf(names []string, size int, tokens bool, list, scope string) *contracts.Page[string] {
	page := &contracts.Page[string]{Items: names}
	if len(names) > size {
		page.Items = names[:size]
		if tokens {
			page.NextToken = encodeToken(list, scope, names[size-1])
		}
	}
	return page
}

// SetQueueAttributes sets attributes on the queue, removing those set to "".
func (s *Service) SetQueueAttributes(ctx context.Context, name string,
	attrs map[string]string,
) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		if err = s.checkRedrive(ctx, tx, name, q.fifo, attrs); err != nil {
			return err
		}
		applyAttributes(q.attrs, attrs)
		return s.saveAttributes(ctx, tx, q)
	})
}

func (s *Service) saveAttributes(ctx context.Context, tx *sql.Tx, q *queueRow) error {
	raw, err := json.Marshal(q.attrs)
	if err != nil {
		return fmt.Errorf("encode attributes: %w", err)
	}
	return exec(ctx, tx, `UPDATE queues SET attributes = ?, modified_at = ? WHERE id = ?`,
		string(raw), stamp(s.now()), q.id)
}

// PurgeQueue deletes every message of the queue, in flight or not, at most
// once in PurgeWindow.
func (s *Service) PurgeQueue(ctx context.Context, name string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		now := s.now()
		if q.purgedAt != 0 && now.Sub(unstamp(q.purgedAt)) < contracts.PurgeWindow {
			return contracts.ErrPurgeInProgress
		}
		if err = purgeMessages(ctx, tx, q.id); err != nil {
			return err
		}
		return exec(ctx, tx, `UPDATE queues SET purged_at = ? WHERE id = ?`, stamp(now), q.id)
	})
}

// purgeMessages deletes the queue's messages and the receive attempts that
// name them.
func purgeMessages(ctx context.Context, tx *sql.Tx, queueID int64) error {
	if err := exec(ctx, tx, `DELETE FROM messages WHERE queue_id = ?`, queueID); err != nil {
		return err
	}
	return exec(ctx, tx, `DELETE FROM receive_attempts WHERE queue_id = ?`, queueID)
}

func putTags(ctx context.Context, tx *sql.Tx, queueID int64, tags map[string]string) error {
	for k, v := range tags {
		if err := exec(ctx, tx,
			`INSERT INTO queue_tags (queue_id, key, value) VALUES (?, ?, ?)
			 ON CONFLICT (queue_id, key) DO UPDATE SET value = excluded.value`,
			queueID, k, v); err != nil {
			return err
		}
	}
	return nil
}

// TagQueue adds tags to the queue, replacing the values of keys it has.
func (s *Service) TagQueue(ctx context.Context, name string, tags map[string]string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		return putTags(ctx, tx, q.id, tags)
	})
}

// UntagQueue removes the tags with the given keys. A key the queue does not
// have is not an error.
func (s *Service) UntagQueue(ctx context.Context, name string, keys []string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err = exec(ctx, tx, `DELETE FROM queue_tags WHERE queue_id = ? AND key = ?`,
				q.id, k); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListQueueTags returns the queue's tags.
func (s *Service) ListQueueTags(ctx context.Context, name string) (map[string]string, error) {
	var out map[string]string
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		type tag struct{ k, v string }
		tags, err := queryAll(ctx, tx, func(r scanner) (tag, error) {
			var t tag
			sErr := r.Scan(&t.k, &t.v)
			return t, sErr
		}, `SELECT key, value FROM queue_tags WHERE queue_id = ?`, q.id)
		if err != nil {
			return err
		}
		out = make(map[string]string, len(tags))
		for _, t := range tags {
			out[t.k] = t.v
		}
		return nil
	})
	return out, err
}

// ListDeadLetterSourceQueues lists, in order, the names of the queues whose
// RedrivePolicy names the queue as their dead-letter queue.
func (s *Service) ListDeadLetterSourceQueues(ctx context.Context, name string,
	page contracts.PageInput,
) (*contracts.Page[string], error) {
	var out *contracts.Page[string]
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := loadQueue(ctx, tx, name); err != nil {
			return err
		}
		after, err := decodeToken(page.NextToken, listDLQSource, name)
		if err != nil {
			return err
		}
		size := page.MaxResults
		if size == 0 {
			size = maxListResults
		}
		names, err := s.sourcesOf(ctx, tx, name, after, size+1)
		if err != nil {
			return err
		}
		out = pageOf(names, size, page.MaxResults > 0, listDLQSource, name)
		if out.Items == nil {
			out.Items = []string{}
		}
		return nil
	})
	return out, err
}

// sourcesOf returns, in order, up to n names after after of the queues whose
// RedrivePolicy targets the queue name.
func (s *Service) sourcesOf(ctx context.Context, tx *sql.Tx, name, after string, n int) (
	[]string, error,
) {
	all, err := queryAll(ctx, tx, scanQueue,
		`SELECT `+queueColumns+` FROM queues WHERE name > ? ORDER BY name`, after)
	if err != nil {
		return nil, err
	}
	arn := s.settings.ARN(name)
	var names []string
	for _, q := range all {
		set, sErr := q.settings()
		if sErr != nil {
			return nil, sErr
		}
		if set.Redrive != nil && set.Redrive.DeadLetterTargetArn == arn {
			names = append(names, q.name)
		}
		if len(names) == n {
			break
		}
	}
	return names, nil
}

// policy is a queue policy as AddPermission writes it.
type policy struct {
	Version   string            `json:"Version"`
	ID        string            `json:"Id"`
	Statement []json.RawMessage `json:"Statement"`
}

type statement struct {
	Principal map[string][]string `json:"Principal"`
	Sid       string              `json:"Sid"`
	Effect    string              `json:"Effect"`
	Resource  string              `json:"Resource"`
	Action    []string            `json:"Action"`
}

// sidOf returns a policy statement's Sid, or "" when it has none.
func sidOf(raw json.RawMessage) string {
	var st struct {
		Sid string `json:"Sid"`
	}
	if json.Unmarshal(raw, &st) != nil {
		return ""
	}
	return st.Sid
}

// queuePolicy reads the queue's Policy, or an empty one when it has none.
func (s *Service) queuePolicy(q *queueRow) (*policy, error) {
	v := q.attrs[contracts.AttrPolicy]
	p := &policy{Version: "2012-10-17", ID: s.settings.ARN(q.name) + "/SQSDefaultPolicy"}
	if v == "" {
		return p, nil
	}
	if err := json.Unmarshal([]byte(v), p); err != nil {
		return nil, &contracts.InvalidError{
			Field: contracts.AttrPolicy, Value: v,
			Reason: "The queue's Policy is not a policy document with a list of statements",
		}
	}
	return p, nil
}

func (s *Service) savePolicy(ctx context.Context, tx *sql.Tx, q *queueRow, p *policy) error {
	if len(p.Statement) == 0 {
		delete(q.attrs, contracts.AttrPolicy)
		return s.saveAttributes(ctx, tx, q)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode policy: %w", err)
	}
	q.attrs[contracts.AttrPolicy] = string(raw)
	return s.saveAttributes(ctx, tx, q)
}

// AddPermission adds a statement to the queue's Policy, as SQS's
// AddPermission generates one: the label as its Sid, allowing the accounts'
// root users the actions on the queue. The policy has no effect here.
func (s *Service) AddPermission(ctx context.Context, name string, perm contracts.Permission) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		p, err := s.queuePolicy(q)
		if err != nil {
			return err
		}
		for _, st := range p.Statement {
			if sidOf(st) == perm.Label {
				return &contracts.InvalidError{
					Field: "Label", Value: perm.Label,
					Reason: "Already exists",
				}
			}
		}
		st := statement{
			Sid: perm.Label, Effect: "Allow", Resource: s.settings.ARN(name),
			Principal: map[string][]string{"AWS": {}},
		}
		for _, id := range perm.AccountIDs {
			st.Principal["AWS"] = append(st.Principal["AWS"], "arn:aws:iam::"+id+":root")
		}
		for _, a := range perm.Actions {
			st.Action = append(st.Action, "SQS:"+a)
		}
		raw, err := json.Marshal(st)
		if err != nil {
			return fmt.Errorf("encode statement: %w", err)
		}
		p.Statement = append(p.Statement, raw)
		return s.savePolicy(ctx, tx, q, p)
	})
}

// RemovePermission removes the statement whose Sid is label from the
// queue's Policy.
func (s *Service) RemovePermission(ctx context.Context, name, label string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		q, err := loadQueue(ctx, tx, name)
		if err != nil {
			return err
		}
		p, err := s.queuePolicy(q)
		if err != nil {
			return err
		}
		i := slices.IndexFunc(p.Statement, func(st json.RawMessage) bool {
			return sidOf(st) == label
		})
		if i < 0 {
			return &contracts.InvalidError{
				Field: "Label", Value: label,
				Reason: "can't find label",
			}
		}
		p.Statement = slices.Delete(p.Statement, i, i+1)
		return s.savePolicy(ctx, tx, q, p)
	})
}
