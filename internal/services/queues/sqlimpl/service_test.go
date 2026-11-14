package sqlimpl

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gasmod/gas"
	"github.com/gasmod/gas/database"
	"github.com/gasmod/gas/migrate"
	_ "modernc.org/sqlite"

	"tule.dev/queues/internal/contracts"
)

var testSettings = contracts.QueueSettings{AccountID: "000000000000", Region: "us-east-1"}

// clock is a clock the tests move by hand.
type clock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newStore opens a store on a fresh database file the way the app wires it,
// on a clock the test moves.
func newStore(t testing.TB) (*Service, *clock) {
	t.Helper()
	ctx := context.Background()
	logger := &gas.NopLogger{}

	cfg := database.DefaultConfig()
	cfg.Database.Dialect = database.DialectSQLite
	cfg.Database.DSN = gas.Secret("file:" + filepath.Join(t.TempDir(), "queues.db"))
	cfg.Database.MaxOpenConns = 1
	cfg.Database.MaxIdleConns = 1

	db, err := database.New(database.WithConfig(cfg))(nil, logger)
	if err != nil {
		t.Fatalf("new database: %v", err)
	}
	if err = db.Init(ctx); err != nil {
		t.Fatalf("init database: %v", err)
	}
	t.Cleanup(func() {
		if cErr := db.Close(ctx); cErr != nil {
			t.Errorf("close database: %v", cErr)
		}
	})
	mm, err := migrate.New()(db, logger)
	if err != nil {
		t.Fatalf("new migrate: %v", err)
	}
	if err = mm.Init(ctx); err != nil {
		t.Fatalf("init migrate: %v", err)
	}

	c := &clock{now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	settings := testSettings
	s := NewService(&settings, c.Now)(db, mm, gas.NewEventBus(), logger)
	if err = s.Init(ctx); err != nil {
		t.Fatalf("init store: %v", err)
	}
	// Cleanups run last first: the store's move tasks stop before the
	// database closes, as gas closes them.
	t.Cleanup(func() {
		if cErr := s.Close(ctx); cErr != nil {
			t.Errorf("close store: %v", cErr)
		}
	})
	if err = mm.RunPendingContext(ctx); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	return s, c
}

func createQueue(t testing.TB, s *Service, name string, fifo bool, attrs map[string]string) {
	t.Helper()
	if _, err := s.CreateQueue(t.Context(), contracts.CreateQueueInput{
		Name: name, FIFO: fifo, Attributes: attrs,
	}); err != nil {
		t.Fatalf("create queue %s: %v", name, err)
	}
}

func send(
	t *testing.T,
	s *Service,
	queue string,
	msgs ...contracts.SendInput,
) []contracts.SendResult {
	t.Helper()
	out, err := s.SendMessages(t.Context(), queue, msgs)
	if err != nil {
		t.Fatalf("send to %s: %v", queue, err)
	}
	return out
}

func receive(t *testing.T, s *Service, queue string, n int) []*contracts.Message {
	t.Helper()
	out, err := s.ReceiveMessages(t.Context(), queue, contracts.ReceiveInput{Max: n})
	if err != nil {
		t.Fatalf("receive from %s: %v", queue, err)
	}
	return out
}

func bodies(msgs []*contracts.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Body)
	}
	return out
}

func requireBodies(t *testing.T, got []*contracts.Message, want ...string) {
	t.Helper()
	if b := bodies(got); !slices.Equal(b, want) {
		t.Fatalf("received %q, want %q", b, want)
	}
}

func intp(n int) *int { return &n }

func TestVisibilityTimeoutHidesAndReturnsAMessage(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q", false, map[string]string{"VisibilityTimeout": "30"})
	send(t, s, "q", contracts.SendInput{Body: "a"})

	first := receive(t, s, "q", 1)
	requireBodies(t, first, "a")
	requireBodies(t, receive(t, s, "q", 1))
	c.advance(30 * time.Second)
	again := receive(t, s, "q", 1)
	requireBodies(t, again, "a")
	if again[0].ReceiveCount != 2 {
		t.Fatalf("receive count = %d, want 2", again[0].ReceiveCount)
	}

	// The first receive's handle is stale: a delete with it succeeds and
	// deletes nothing, and a visibility change is refused.
	errs, err := s.DeleteMessages(t.Context(), "q", []string{first[0].ReceiptHandle})
	if err != nil || errs[0] != nil {
		t.Fatalf("delete with a stale handle = %v, %v, want success", errs, err)
	}
	errs, err = s.ChangeVisibility(t.Context(), "q",
		[]contracts.VisibilityChange{{ReceiptHandle: first[0].ReceiptHandle}})
	if err != nil || !errors.Is(errs[0], contracts.ErrMessageNotInflight) {
		t.Fatalf("change visibility with a stale handle = %v, %v, want not in flight", errs, err)
	}
	q, err := s.GetQueue(t.Context(), "q")
	if err != nil || q.Counts.InFlight != 1 {
		t.Fatalf("counts = %+v, %v, want the message still in flight", q, err)
	}

	errs, err = s.DeleteMessages(t.Context(), "q", []string{again[0].ReceiptHandle})
	if err != nil || errs[0] != nil {
		t.Fatalf("delete = %v, %v", errs, err)
	}
	c.advance(time.Minute)
	requireBodies(t, receive(t, s, "q", 1))
}

func TestChangeVisibilityToZeroMakesAMessageVisible(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q", false, nil)
	send(t, s, "q", contracts.SendInput{Body: "a"})
	m := receive(t, s, "q", 1)

	errs, err := s.ChangeVisibility(t.Context(), "q",
		[]contracts.VisibilityChange{{ReceiptHandle: m[0].ReceiptHandle, Timeout: 0}})
	if err != nil || errs[0] != nil {
		t.Fatalf("change visibility = %v, %v", errs, err)
	}
	requireBodies(t, receive(t, s, "q", 1), "a")
}

func TestChangeVisibilityIsBoundedByTwelveHours(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q", false, map[string]string{"VisibilityTimeout": "7200"})
	send(t, s, "q", contracts.SendInput{Body: "a"})
	m := receive(t, s, "q", 1)
	c.advance(time.Hour)

	errs, err := s.ChangeVisibility(t.Context(), "q",
		[]contracts.VisibilityChange{{ReceiptHandle: m[0].ReceiptHandle, Timeout: 43200}})
	var invalid *contracts.InvalidError
	if err != nil || !errors.As(errs[0], &invalid) {
		t.Fatalf("change past 12h from the receive = %v, %v, want an invalid value", errs, err)
	}
}

func TestAHandleOfAnotherQueueIsInvalid(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "a", false, nil)
	createQueue(t, s, "b", false, nil)
	send(t, s, "a", contracts.SendInput{Body: "m"})
	m := receive(t, s, "a", 1)

	for _, h := range []string{m[0].ReceiptHandle, "bogus"} {
		errs, err := s.DeleteMessages(t.Context(), "b", []string{h})
		var receipt *contracts.ReceiptError
		if err != nil || !errors.As(errs[0], &receipt) {
			t.Fatalf("delete %q from b = %v, %v, want an invalid receipt", h, errs, err)
		}
	}
}

func TestDelayHoldsAMessageBack(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q", false, map[string]string{"DelaySeconds": "10"})
	send(t, s, "q", contracts.SendInput{Body: "queue delay"},
		contracts.SendInput{Body: "own delay", DelaySeconds: intp(0)})

	requireBodies(t, receive(t, s, "q", 10), "own delay")
	q, err := s.GetQueue(t.Context(), "q")
	if err != nil || q.Counts.Delayed != 1 {
		t.Fatalf("counts = %+v, %v, want one delayed", q, err)
	}
	c.advance(10 * time.Second)
	requireBodies(t, receive(t, s, "q", 10), "queue delay")
}

func TestRetentionDeletesOldMessages(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q", false, map[string]string{"MessageRetentionPeriod": "60"})
	send(t, s, "q", contracts.SendInput{Body: "old"})
	c.advance(30 * time.Second)
	send(t, s, "q", contracts.SendInput{Body: "new"})
	c.advance(30 * time.Second)

	requireBodies(t, receive(t, s, "q", 10), "new")
}

func TestRedriveMovesAMessagePastMaxReceiveCount(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, map[string]string{
		"VisibilityTimeout": "1",
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq",` +
			`"maxReceiveCount":2}`,
	})
	send(t, s, "src", contracts.SendInput{Body: "poison"})

	for range 2 {
		requireBodies(t, receive(t, s, "src", 1), "poison")
		c.advance(time.Second)
	}
	// The third receive would take it past 2: it moves instead.
	requireBodies(t, receive(t, s, "src", 1))
	moved := receive(t, s, "dlq", 1)
	requireBodies(t, moved, "poison")
	if moved[0].DeadLetterSourceARN != "arn:aws:sqs:us-east-1:000000000000:src" {
		t.Fatalf("dead-letter source = %q", moved[0].DeadLetterSourceARN)
	}
	if moved[0].ReceiveCount != 3 {
		t.Fatalf("receive count in the dead-letter queue = %d, want 3", moved[0].ReceiveCount)
	}

	sources, err := s.ListDeadLetterSourceQueues(t.Context(), "dlq", contracts.PageInput{})
	if err != nil || len(sources.Items) != 1 || sources.Items[0] != "src" {
		t.Fatalf("dead-letter sources = %+v, %v, want src", sources, err)
	}
}

func TestRedriveChecksTheDeadLetterQueue(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "fifo-dlq.fifo", true, nil)
	createQueue(t, s, "closed", false,
		map[string]string{"RedriveAllowPolicy": `{"redrivePermission":"denyAll"}`})
	createQueue(t, s, "src", false, nil)

	for _, target := range []string{"missing", "fifo-dlq.fifo", "closed", "src"} {
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:` + target +
			`","maxReceiveCount":1}`
		err := s.SetQueueAttributes(t.Context(), "src", map[string]string{"RedrivePolicy": policy})
		var invalid *contracts.InvalidError
		if !errors.As(err, &invalid) {
			t.Fatalf("redrive to %s = %v, want an invalid RedrivePolicy", target, err)
		}
	}
}

func fifoMsg(body, group string) contracts.SendInput {
	return contracts.SendInput{Body: body, GroupID: group, DeduplicationID: body}
}

func TestFIFODeduplicatesWithinTheInterval(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q.fifo", true, nil)

	first := send(t, s, "q.fifo", fifoMsg("a", "g"))
	dup := send(t, s, "q.fifo", fifoMsg("a", "g"))
	if dup[0] != first[0] {
		t.Fatalf("duplicate = %+v, want the first send's %+v", dup[0], first[0])
	}
	requireBodies(t, receive(t, s, "q.fifo", 10), "a")

	c.advance(5 * time.Minute)
	again := send(t, s, "q.fifo", fifoMsg("a", "g"))
	if again[0].MessageID == first[0].MessageID {
		t.Fatal("a send after the deduplication interval was deduplicated")
	}
	if a, _ := strconv.ParseInt(again[0].SequenceNumber, 10, 64); a <= mustInt(t,
		first[0].SequenceNumber) {
		t.Fatalf("sequence %s does not follow %s", again[0].SequenceNumber, first[0].SequenceNumber)
	}
}

func mustInt(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("sequence number %q: %v", s, err)
	}
	return n
}

func TestFIFODeduplicationScopeMessageGroup(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q.fifo", true, map[string]string{"DeduplicationScope": "messageGroup"})

	send(t, s, "q.fifo", fifoMsg("a", "g1"), fifoMsg("a", "g2"))
	q, err := s.GetQueue(t.Context(), "q.fifo")
	if err != nil || q.Counts.Visible != 2 {
		t.Fatalf("counts = %+v, %v, want the same ID kept once per group", q, err)
	}
}

func TestFIFOHoldsAGroupWhileAMessageIsInFlight(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q.fifo", true, nil)
	send(t, s, "q.fifo", fifoMsg("a1", "a"), fifoMsg("b1", "b"), fifoMsg("a2", "a"),
		fifoMsg("b2", "b"))

	first := receive(t, s, "q.fifo", 1)
	requireBodies(t, first, "a1")
	// Group a is held, so the next receive starts group b, in order.
	requireBodies(t, receive(t, s, "q.fifo", 10), "b1", "b2")
	requireBodies(t, receive(t, s, "q.fifo", 10))

	errs, err := s.DeleteMessages(t.Context(), "q.fifo", []string{first[0].ReceiptHandle})
	if err != nil || errs[0] != nil {
		t.Fatalf("delete = %v, %v", errs, err)
	}
	requireBodies(t, receive(t, s, "q.fifo", 10), "a2")
}

func TestFIFORetriedAttemptReturnsTheSameMessages(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q.fifo", true, nil)
	send(t, s, "q.fifo", fifoMsg("a", "g"))

	in := contracts.ReceiveInput{Max: 1, AttemptID: "attempt-1"}
	first, err := s.ReceiveMessages(t.Context(), "q.fifo", in)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	retry, err := s.ReceiveMessages(t.Context(), "q.fifo", in)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	requireBodies(t, retry, "a")
	if retry[0].ReceiptHandle != first[0].ReceiptHandle || retry[0].ReceiveCount != 1 {
		t.Fatalf("retry = %+v, want the first receive's handle and count", retry[0])
	}
}

func TestPurgeAndDeleteHoldTheirWindows(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q", false, nil)
	send(t, s, "q", contracts.SendInput{Body: "a"})

	if err := s.PurgeQueue(t.Context(), "q"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if err := s.PurgeQueue(t.Context(), "q"); !errors.Is(err, contracts.ErrPurgeInProgress) {
		t.Fatalf("second purge = %v, want ErrPurgeInProgress", err)
	}
	c.advance(time.Minute)
	if err := s.PurgeQueue(t.Context(), "q"); err != nil {
		t.Fatalf("purge a minute later: %v", err)
	}

	if err := s.DeleteQueue(t.Context(), "q"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := s.CreateQueue(t.Context(), contracts.CreateQueueInput{Name: "q"})
	if !errors.Is(err, contracts.ErrQueueDeletedRecently) {
		t.Fatalf("create after delete = %v, want ErrQueueDeletedRecently", err)
	}
	c.advance(time.Minute)
	createQueue(t, s, "q", false, nil)
}

func TestCreateQueueIsIdempotentForTheSameAttributes(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q", false, map[string]string{"VisibilityTimeout": "40"})
	createQueue(t, s, "q", false, map[string]string{"VisibilityTimeout": "40"})
	createQueue(t, s, "q", false, nil)
	_, err := s.CreateQueue(t.Context(), contracts.CreateQueueInput{
		Name: "q", Attributes: map[string]string{"VisibilityTimeout": "41"},
	})
	if !errors.Is(err, contracts.ErrQueueExists) {
		t.Fatalf("create with another value = %v, want ErrQueueExists", err)
	}
}

func TestListQueuesPagesOnlyWithMaxResults(t *testing.T) {
	s, _ := newStore(t)
	for _, n := range []string{"a1", "a2", "a3", "b1"} {
		createQueue(t, s, n, false, nil)
	}
	list := func(prefix string, page contracts.PageInput) *contracts.Page[string] {
		t.Helper()
		out, err := s.ListQueues(t.Context(), prefix, page)
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		return out
	}
	if all := list("a", contracts.PageInput{}); len(all.Items) != 3 || all.NextToken != "" {
		t.Fatalf("list a = %+v, want 3 names and no token", all)
	}
	page := list("a", contracts.PageInput{MaxResults: 2})
	if len(page.Items) != 2 || page.NextToken == "" {
		t.Fatalf("first page = %+v", page)
	}
	rest := list("a", contracts.PageInput{MaxResults: 2, NextToken: page.NextToken})
	if !slices.Equal(rest.Items, []string{"a3"}) || rest.NextToken != "" {
		t.Fatalf("second page = %+v", rest)
	}
	_, err := s.ListQueues(t.Context(), "b",
		contracts.PageInput{MaxResults: 2, NextToken: page.NextToken})
	if !errors.Is(err, contracts.ErrInvalidToken) {
		t.Fatalf("a token of another prefix = %v, want ErrInvalidToken", err)
	}
}

func TestALongPollWakesOnASend(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q", false, nil)

	done := make(chan []*contracts.Message)
	go func() {
		out, _ := s.ReceiveMessages(context.Background(), "q",
			contracts.ReceiveInput{Max: 1, WaitSeconds: intp(20)})
		done <- out
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	send(t, s, "q", contracts.SendInput{Body: "a"})
	select {
	case got := <-done:
		requireBodies(t, got, "a")
		if took := time.Since(start); took > 2*time.Second {
			t.Fatalf("the poll returned %s after the send", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll did not return after the send")
	}
}

func TestStoppingEndsALongPoll(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q", false, nil)

	done := make(chan error)
	go func() {
		_, err := s.ReceiveMessages(context.Background(), "q",
			contracts.ReceiveInput{Max: 1, WaitSeconds: intp(20)})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	s.signals.stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receive = %v, want an empty answer", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll did not end when the store stopped")
	}
}

func TestListMessagesFiltersAndPages(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q", false, nil)
	send(t, s, "q", contracts.SendInput{Body: "a"}, contracts.SendInput{Body: "b"},
		contracts.SendInput{Body: "c"})
	receive(t, s, "q", 1)
	list := func(f contracts.MessageFilter, page contracts.PageInput) *contracts.Page[*contracts.Message] {
		t.Helper()
		out, err := s.ListMessages(t.Context(), "q", f, page)
		if err != nil {
			t.Fatalf("list messages: %v", err)
		}
		return out
	}

	inFlight := list(contracts.MessageFilter{State: contracts.StateInFlight},
		contracts.PageInput{MaxResults: 10})
	requireBodies(t, inFlight.Items, "a")
	page := list(contracts.MessageFilter{}, contracts.PageInput{MaxResults: 2})
	if len(page.Items) != 2 || page.NextToken == "" {
		t.Fatalf("first page = %+v", page)
	}
	rest := list(contracts.MessageFilter{},
		contracts.PageInput{MaxResults: 2, NextToken: page.NextToken})
	requireBodies(t, rest.Items, "c")
	if rest.Items[0].State != contracts.StateVisible {
		t.Fatalf("state = %s, want visible", rest.Items[0].State)
	}
	// Listing changes nothing a receive sees.
	requireBodies(t, receive(t, s, "q", 10), "b", "c")
}

func TestFIFOHoldsAGroupWhoseHeadWasNackedWhileItsTailIsInFlight(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q.fifo", true, nil)
	send(t, s, "q.fifo", fifoMsg("m1", "g"), fifoMsg("m2", "g"))
	both := receive(t, s, "q.fifo", 10)
	requireBodies(t, both, "m1", "m2")

	errs, err := s.ChangeVisibility(t.Context(), "q.fifo",
		[]contracts.VisibilityChange{{ReceiptHandle: both[0].ReceiptHandle, Timeout: 0}})
	if err != nil || errs[0] != nil {
		t.Fatalf("nack m1 = %v, %v", errs, err)
	}
	// m2 is still in flight, so group g is held.
	requireBodies(t, receive(t, s, "q.fifo", 10))

	errs, err = s.DeleteMessages(t.Context(), "q.fifo", []string{both[1].ReceiptHandle})
	if err != nil || errs[0] != nil {
		t.Fatalf("delete m2 = %v, %v", errs, err)
	}
	requireBodies(t, receive(t, s, "q.fifo", 10), "m1")
}

func TestFIFOHoldsAGroupWhoseTailWasNackedWhileItsHeadIsInFlight(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q.fifo", true, nil)
	send(t, s, "q.fifo", fifoMsg("m1", "g"), fifoMsg("m2", "g"))
	both := receive(t, s, "q.fifo", 10)

	errs, err := s.ChangeVisibility(t.Context(), "q.fifo",
		[]contracts.VisibilityChange{{ReceiptHandle: both[1].ReceiptHandle, Timeout: 0}})
	if err != nil || errs[0] != nil {
		t.Fatalf("nack m2 = %v, %v", errs, err)
	}
	requireBodies(t, receive(t, s, "q.fifo", 10))
}

func TestConcurrentFIFOReceiversNeverShareAGroup(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q.fifo", true, nil)
	groups := []string{"a", "b", "c"}
	for i := range 5 {
		for _, g := range groups {
			send(t, s, "q.fifo", fifoMsg(g+strconv.Itoa(i), g))
		}
	}

	var (
		mu  sync.Mutex
		got []*contracts.Message
		wg  sync.WaitGroup
	)
	for range 20 {
		wg.Go(func() {
			out, err := s.ReceiveMessages(context.Background(), "q.fifo",
				contracts.ReceiveInput{Max: 1})
			if err != nil {
				t.Errorf("receive: %v", err)
				return
			}
			mu.Lock()
			got = append(got, out...)
			mu.Unlock()
		})
	}
	wg.Wait()

	// Nothing is deleted, so each group gives up its head alone.
	seen := map[string]string{}
	for _, m := range got {
		if prev, ok := seen[m.GroupID]; ok {
			t.Fatalf("group %s had %s and %s in flight at once", m.GroupID, prev, m.Body)
		}
		seen[m.GroupID] = m.Body
	}
	for _, g := range groups {
		if seen[g] != g+"0" {
			t.Fatalf("received %v, want each group's first message", seen)
		}
	}
}

func TestAReceiveWithNoVisibilityTimeoutTakesAMessageOnce(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "q", false, nil)
	send(t, s, "q", contracts.SendInput{Body: "a"})

	got, err := s.ReceiveMessages(t.Context(), "q",
		contracts.ReceiveInput{Max: 10, VisibilityTimeout: intp(0)})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	requireBodies(t, got, "a")
}

func TestARetriedAttemptKeepsTheFirstReceiveTime(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "q.fifo", true, map[string]string{"VisibilityTimeout": "600"})
	send(t, s, "q.fifo", fifoMsg("a", "g"))
	in := contracts.ReceiveInput{Max: 1, AttemptID: "attempt"}
	if _, err := s.ReceiveMessages(t.Context(), "q.fifo", in); err != nil {
		t.Fatalf("receive: %v", err)
	}
	c.advance(4 * time.Minute)
	retry, err := s.ReceiveMessages(t.Context(), "q.fifo", in)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	requireBodies(t, retry, "a")

	// 12h from the first receive is 11h56m from now; asking for 11h56m30s
	// goes past it.
	errs, err := s.ChangeVisibility(t.Context(), "q.fifo", []contracts.VisibilityChange{{
		ReceiptHandle: retry[0].ReceiptHandle,
		Timeout:       int((contracts.MaxVisibility - 4*time.Minute + 30*time.Second).Seconds()),
	}})
	var invalid *contracts.InvalidError
	if err != nil || !errors.As(errs[0], &invalid) {
		t.Fatalf("change past 12h from the first receive = %v, %v, want an invalid value",
			errs, err)
	}
}
