package sqlimpl

import "github.com/gasmod/gas"

// migrations is the store's schema, applied by gas/migrate in version order.
//
// Times are Unix milliseconds. A queue keeps its settable attributes as one
// JSON object, normalized and with every default filled in, as
// GetQueueAttributes reports them. Queue and message IDs are AUTOINCREMENT,
// so an ID is never reused: a receipt handle names the queue and the row of
// its message, and a queue deleted and created again, or a message deleted,
// leaves no row a stale handle could name. A message's row ID is its place
// in the queue.
//
// A message is delayed while visible_at is ahead and it was never received,
// in flight while visible_at is ahead and it was, and visible otherwise.
// receipt is the nonce of its latest receive, which the receive's handle
// carries.
var migrations = []gas.Migration{
	{
		Version:     "0001",
		Description: "create the queues schema",
		Up: `
CREATE TABLE queues (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	name          TEXT NOT NULL UNIQUE,
	fifo          INTEGER NOT NULL,
	attributes    TEXT NOT NULL,
	created_at    INTEGER NOT NULL,
	modified_at   INTEGER NOT NULL,
	purged_at     INTEGER,
	next_sequence INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE queue_tags (
	queue_id INTEGER NOT NULL,
	key      TEXT NOT NULL,
	value    TEXT NOT NULL,
	PRIMARY KEY (queue_id, key)
);

CREATE TABLE deleted_queues (
	name       TEXT PRIMARY KEY,
	deleted_at INTEGER NOT NULL
);

CREATE TABLE messages (
	id                INTEGER PRIMARY KEY AUTOINCREMENT,
	queue_id          INTEGER NOT NULL,
	message_id        TEXT NOT NULL,
	body              TEXT NOT NULL,
	md5_body          TEXT NOT NULL,
	attributes        TEXT NOT NULL,
	md5_attributes    TEXT NOT NULL,
	system_attributes TEXT NOT NULL,
	md5_system        TEXT NOT NULL,
	sender_id         TEXT NOT NULL,
	sent_at           INTEGER NOT NULL,
	enqueued_at       INTEGER NOT NULL,
	visible_at        INTEGER NOT NULL,
	receive_count     INTEGER NOT NULL DEFAULT 0,
	first_received_at INTEGER NOT NULL DEFAULT 0,
	received_at       INTEGER NOT NULL DEFAULT 0,
	receipt           TEXT NOT NULL DEFAULT '',
	group_id          TEXT NOT NULL,
	dedup_id          TEXT NOT NULL,
	sequence_number   TEXT NOT NULL,
	dlq_source_arn    TEXT NOT NULL DEFAULT ''
);
-- The inspection listing pages a queue in its order.
CREATE INDEX messages_queue ON messages (queue_id, id);
-- A standard receive takes visible messages in the order they became
-- visible; the in-flight check and the next-due time read the rest of the
-- same range.
CREATE INDEX messages_visible ON messages (queue_id, visible_at, id, receive_count);
-- A FIFO receive walks the groups, finds each one's head, and checks it has
-- nothing in flight, each a seek.
CREATE INDEX messages_group_order ON messages (queue_id, group_id, id);
CREATE INDEX messages_group_visible ON messages (queue_id, group_id, visible_at, receive_count);
-- Retention deletes from the oldest enqueue time.
CREATE INDEX messages_enqueued ON messages (queue_id, enqueued_at);

CREATE TABLE deduplication (
	queue_id        INTEGER NOT NULL,
	group_id        TEXT NOT NULL,
	dedup_id        TEXT NOT NULL,
	message_id      TEXT NOT NULL,
	sequence_number TEXT NOT NULL,
	expires_at      INTEGER NOT NULL,
	PRIMARY KEY (queue_id, group_id, dedup_id)
);
CREATE INDEX deduplication_expiry ON deduplication (expires_at);

CREATE TABLE receive_attempts (
	queue_id   INTEGER NOT NULL,
	attempt_id TEXT NOT NULL,
	expires_at INTEGER NOT NULL,
	visibility INTEGER NOT NULL,
	receipts   TEXT NOT NULL,
	PRIMARY KEY (queue_id, attempt_id)
);
CREATE INDEX receive_attempts_expiry ON receive_attempts (expires_at);
`,
		Down: `
DROP TABLE receive_attempts;
DROP TABLE deduplication;
DROP TABLE messages;
DROP TABLE deleted_queues;
DROP TABLE queue_tags;
DROP TABLE queues;
`,
	},
	{
		Version:     "0002",
		Description: "create the message move tasks",
		// A task belongs to its source queue, queue_id, and goes with it.
		// up_to is the last message row it may move. max_per_second is 0
		// for the system-optimized rate, and destination_arn "" for a task
		// that moves each message back to the queue it came from. run is
		// the server run that started the task: a task left running or
		// canceling by an earlier run is no longer moving anything.
		Up: `
CREATE TABLE move_tasks (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	queue_id        INTEGER NOT NULL,
	handle          TEXT NOT NULL UNIQUE,
	destination_arn TEXT NOT NULL,
	max_per_second  INTEGER NOT NULL,
	status          TEXT NOT NULL,
	to_move         INTEGER NOT NULL,
	moved           INTEGER NOT NULL DEFAULT 0,
	up_to           INTEGER NOT NULL,
	failure_reason  TEXT NOT NULL DEFAULT '',
	started_at      INTEGER NOT NULL,
	run             TEXT NOT NULL
);
CREATE INDEX move_tasks_queue ON move_tasks (queue_id, id);
CREATE INDEX move_tasks_status ON move_tasks (status);
`,
		Down: `
DROP TABLE move_tasks;
`,
	},
}
