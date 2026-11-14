package sqlimpl

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"tule.dev/queues/internal/contracts"
)

// toDLQ returns the attributes of a source queue that moves a message to
// the dead-letter queue dlq after one receive.
func toDLQ() map[string]string {
	return map[string]string{
		"VisibilityTimeout": "1",
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq",` +
			`"maxReceiveCount":1}`,
	}
}

// deadLetter sends bodies to src and has each moved to its dead-letter
// queue, receiving them once and then once more on the clock c.
func deadLetter(t *testing.T, s *Service, c *clock, src string, bodies ...string) {
	t.Helper()
	for _, b := range bodies {
		send(t, s, src, contracts.SendInput{Body: b})
	}
	requireBodies(t, receive(t, s, src, 10), bodies...)
	c.advance(time.Second)
	requireBodies(t, receive(t, s, src, 10))
}

// startMove starts a task from source to destination ("" for the sources)
// at rate (0 for none) on what MoveScope gives now.
func startMove(t *testing.T, s *Service, source, destination string, rate int) string {
	t.Helper()
	scope, err := s.MoveScope(t.Context(), source)
	if err != nil {
		t.Fatalf("move scope of %s: %v", source, err)
	}
	in := contracts.StartMoveInput{Source: source, Destination: destination, UpTo: scope.UpTo}
	if rate > 0 {
		in.MaxPerSecond = &rate
	}
	handle, err := s.StartMoveTask(t.Context(), in)
	if err != nil {
		t.Fatalf("start a move task on %s: %v", source, err)
	}
	return handle
}

// waitMove waits for the latest task of source to end, and returns it.
func waitMove(t *testing.T, s *Service, source string) *contracts.MoveTask {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		tasks, err := s.ListMoveTasks(t.Context(), source, 1)
		if err != nil || len(tasks) != 1 {
			t.Fatalf("list the tasks of %s = %v, %v", source, tasks, err)
		}
		if st := tasks[0].Status; st != contracts.MoveTaskRunning &&
			st != contracts.MoveTaskCancelling {
			return tasks[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("the task on %s did not end: %+v", source, tasks[0])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAMoveTaskSendsEachMessageBackToItsSource(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "a", false, toDLQ())
	createQueue(t, s, "b", false, toDLQ())
	deadLetter(t, s, c, "a", "a1", "a2")
	deadLetter(t, s, c, "b", "b1")

	scope, err := s.MoveScope(t.Context(), "dlq")
	if err != nil || !slices.Equal(scope.Origins, []string{"a", "b"}) {
		t.Fatalf("move scope = %+v, %v, want the origins a and b", scope, err)
	}
	startMove(t, s, "dlq", "", 0)
	done := waitMove(t, s, "dlq")
	if done.Status != contracts.MoveTaskCompleted || done.Moved != 3 || done.ToMove != 3 ||
		done.DestinationARN != "" || done.MaxPerSecond != nil {
		t.Fatalf("ended task = %+v, want 3 of 3 moved to the sources", done)
	}

	requireBodies(t, receive(t, s, "dlq", 10))
	requireRedriven(t, receive(t, s, "a", 10), "a1", "a2")
	requireRedriven(t, receive(t, s, "b", 10), "b1")
}

// requireRedriven fails unless got are bodies, each a new message: received
// for the first time, with no dead-letter source.
func requireRedriven(t *testing.T, got []*contracts.Message, bodies ...string) {
	t.Helper()
	requireBodies(t, got, bodies...)
	for _, m := range got {
		if m.ReceiveCount != 1 || m.DeadLetterSourceARN != "" {
			t.Fatalf("redriven message = %+v, want a new message", m)
		}
	}
}

func TestAMoveTaskMovesOnlyTheMessagesItWasStartedOn(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	createQueue(t, s, "dest", false, nil)
	send(t, s, "dlq", contracts.SendInput{Body: "before"})
	scope, err := s.MoveScope(t.Context(), "dlq")
	if err != nil {
		t.Fatalf("move scope: %v", err)
	}
	send(t, s, "dlq", contracts.SendInput{Body: "after"})

	if _, err = s.StartMoveTask(t.Context(), contracts.StartMoveInput{
		Source: "dlq", Destination: "dest", UpTo: scope.UpTo,
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if done := waitMove(t, s, "dlq"); done.Moved != 1 || done.ToMove != 1 {
		t.Fatalf("ended task = %+v, want the one message before its scope", done)
	}
	requireBodies(t, receive(t, s, "dest", 10), "before")
	requireBodies(t, receive(t, s, "dlq", 10), "after")
}

func TestAMoveTaskFailsOnAMessageWithNoSource(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	deadLetter(t, s, c, "src", "redriven")
	send(t, s, "dlq", contracts.SendInput{Body: "sent straight"})

	startMove(t, s, "dlq", "", 0)
	done := waitMove(t, s, "dlq")
	if done.Status != contracts.MoveTaskFailed || done.Moved != 1 || done.FailureReason == "" {
		t.Fatalf("ended task = %+v, want failed after one message", done)
	}
	requireBodies(t, receive(t, s, "src", 10), "redriven")
	requireBodies(t, receive(t, s, "dlq", 10), "sent straight")
}

func TestAMoveTaskToAFIFOQueueTakesItsSequenceNumbers(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq.fifo", true, nil)
	createQueue(t, s, "src.fifo", true, map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq.fifo",` +
			`"maxReceiveCount":1}`,
	})
	createQueue(t, s, "dest.fifo", true, nil)
	send(t, s, "dlq.fifo", fifoMsg("x", "g"), fifoMsg("y", "g"))
	first := send(t, s, "dest.fifo", fifoMsg("own", "g"))

	startMove(t, s, "dlq.fifo", "dest.fifo", 0)
	if done := waitMove(t, s, "dlq.fifo"); done.Status != contracts.MoveTaskCompleted {
		t.Fatalf("ended task = %+v", done)
	}
	got := receive(t, s, "dest.fifo", 10)
	requireBodies(t, got, "own", "x", "y")
	if got[0].SequenceNumber != first[0].SequenceNumber ||
		got[1].SequenceNumber <= got[0].SequenceNumber ||
		got[2].SequenceNumber <= got[1].SequenceNumber {
		t.Fatalf("sequence numbers %s %s %s, want the destination's, in order",
			got[0].SequenceNumber, got[1].SequenceNumber, got[2].SequenceNumber)
	}
}

func TestStartMoveTaskRefusals(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	createQueue(t, s, "plain", false, nil)
	createQueue(t, s, "dest.fifo", true, nil)
	start := func(source, destination string) error {
		_, err := s.StartMoveTask(t.Context(), contracts.StartMoveInput{
			Source: source, Destination: destination,
		})
		return err
	}
	var invalid *contracts.InvalidError
	for _, tt := range []struct {
		check       func(error) bool
		source      string
		destination string
	}{
		{source: "plain", check: func(err error) bool {
			return errors.Is(err, contracts.ErrNotDeadLetterQueue)
		}},
		{source: "missing", check: func(err error) bool {
			return errors.Is(err, contracts.ErrQueueNotFound)
		}},
		{source: "dlq", destination: "missing", check: func(err error) bool {
			return errors.Is(err, contracts.ErrQueueNotFound)
		}},
		{source: "dlq", destination: "dlq", check: func(err error) bool {
			return errors.As(err, &invalid)
		}},
		{source: "dlq", destination: "dest.fifo", check: func(err error) bool {
			return errors.As(err, &invalid)
		}},
	} {
		if err := start(tt.source, tt.destination); !tt.check(err) {
			t.Errorf("start from %s to %q = %v", tt.source, tt.destination, err)
		}
	}
	tasks, err := s.ListMoveTasks(t.Context(), "dlq", 10)
	if err != nil || len(tasks) != 0 {
		t.Fatalf("tasks after the refusals = %v, %v, want none", tasks, err)
	}
}

func TestCancelMoveTask(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	for range 5 {
		send(t, s, "dlq", contracts.SendInput{Body: "m"})
	}

	handle := startMove(t, s, "dlq", "src", 1)
	if _, err := s.StartMoveTask(t.Context(), contracts.StartMoveInput{
		Source: "dlq", Destination: "src",
	}); !errors.Is(err, contracts.ErrMoveTaskActive) {
		t.Fatalf("a second start = %v, want an active task", err)
	}
	moved, err := s.CancelMoveTask(t.Context(), "dlq", handle)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	done := waitMove(t, s, "dlq")
	if done.Status != contracts.MoveTaskCancelled || done.Moved < moved || done.Moved >= 5 {
		t.Fatalf("ended task = %+v after a cancel that reported %d moved", done, moved)
	}
	if _, err = s.CancelMoveTask(t.Context(), "dlq", handle); !errors.Is(err,
		contracts.ErrMoveTaskNotRunning) {
		t.Fatalf("a second cancel = %v, want not running", err)
	}
	if _, err = s.CancelMoveTask(t.Context(), "dlq", "other"); !errors.Is(err,
		contracts.ErrMoveTaskNotFound) {
		t.Fatalf("cancel of an unknown handle = %v, want not found", err)
	}
}

func TestListMoveTasksReturnsTheMostRecentOldestFirst(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	for rate := 1; rate <= 3; rate++ {
		startMove(t, s, "dlq", "src", rate*10)
		waitMove(t, s, "dlq")
	}
	tasks, err := s.ListMoveTasks(t.Context(), "dlq", 2)
	if err != nil || len(tasks) != 2 || *tasks[0].MaxPerSecond != 20 ||
		*tasks[1].MaxPerSecond != 30 {
		t.Fatalf("tasks = %+v, %v, want the rates 20 and 30", tasks, err)
	}
}

func TestAMoveTaskLeftRunningFailsOnTheNextRun(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	for range 5 {
		send(t, s, "dlq", contracts.SendInput{Body: "m"})
	}
	startMove(t, s, "dlq", "src", 1)
	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The next run, on the same database.
	next := &Service{
		db: s.db, settings: s.settings, now: s.now, logger: s.logger,
		signals: newSignals(), movers: newMovers(),
	}
	next.movers.begin("next")
	t.Cleanup(func() {
		if err := next.Close(context.Background()); err != nil {
			t.Errorf("close the next run: %v", err)
		}
	})
	tasks, err := next.ListMoveTasks(t.Context(), "dlq", 1)
	if err != nil || len(tasks) != 1 || tasks[0].Status != contracts.MoveTaskFailed ||
		tasks[0].FailureReason != reasonInterrupted {
		t.Fatalf("tasks on the next run = %+v, %v, want the task failed", tasks, err)
	}
	if moved := tasks[0].Moved; moved >= 5 {
		t.Fatalf("the interrupted task moved %d of 5", moved)
	}
	startMove(t, next, "dlq", "src", 0)
	if done := waitMove(t, next, "dlq"); done.Status != contracts.MoveTaskCompleted {
		t.Fatalf("a task of the next run = %+v, want completed", done)
	}
}

func TestARestartedServiceRunsNewMoveTasks(t *testing.T) {
	s, _ := newStore(t)
	createQueue(t, s, "dlq", false, nil)
	createQueue(t, s, "src", false, toDLQ())
	for range 5 {
		send(t, s, "dlq", contracts.SendInput{Body: "m"})
	}
	startMove(t, s, "dlq", "src", 1)
	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
	// A closed service runs no task, so it starts none.
	if _, err := s.StartMoveTask(t.Context(), contracts.StartMoveInput{
		Source: "dlq", Destination: "src", UpTo: 1 << 40,
	}); !errors.Is(err, errStopped) {
		t.Fatalf("start on a closed service = %v, want refused", err)
	}

	// gas's RestartService calls Init again on the same instance.
	if err := s.Init(t.Context()); err != nil {
		t.Fatalf("init again: %v", err)
	}
	tasks, err := s.ListMoveTasks(t.Context(), "dlq", 1)
	if err != nil || len(tasks) != 1 || tasks[0].Status != contracts.MoveTaskFailed {
		t.Fatalf("tasks after the restart = %+v, %v, want the closed run's task failed",
			tasks, err)
	}
	left := 5 - tasks[0].Moved
	startMove(t, s, "dlq", "src", 0)
	if done := waitMove(t, s, "dlq"); done.Status != contracts.MoveTaskCompleted ||
		done.Moved != left {
		t.Fatalf("a task after the restart = %+v, want the %d left moved", done, left)
	}
}

func TestAMoveTaskHoldsAFIFOGroupWithAMessageInFlight(t *testing.T) {
	s, c := newStore(t)
	createQueue(t, s, "dlq.fifo", true, nil)
	createQueue(t, s, "src.fifo", true, map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq.fifo",` +
			`"maxReceiveCount":1}`,
	})
	createQueue(t, s, "dest.fifo", true, nil)
	send(t, s, "dlq.fifo", fifoMsg("held-1", "held"), fifoMsg("held-2", "held"),
		fifoMsg("free", "free"))
	// held-1 is in flight, which holds held-2 behind it.
	if _, err := s.ReceiveMessages(t.Context(), "dlq.fifo", contracts.ReceiveInput{
		Max: 1, VisibilityTimeout: intp(30),
	}); err != nil {
		t.Fatalf("receive: %v", err)
	}

	startMove(t, s, "dlq.fifo", "dest.fifo", 0)
	waitMoved(t, s, "dlq.fifo", 1)
	// Two steps later the held group is still held, and the task waits.
	time.Sleep(3 * heldPause)
	tasks, err := s.ListMoveTasks(t.Context(), "dlq.fifo", 1)
	if err != nil || tasks[0].Status != contracts.MoveTaskRunning || tasks[0].Moved != 1 {
		t.Fatalf("task while a group is held = %+v, %v, want running with 1 moved", tasks, err)
	}

	// Once held-1's visibility ends, the group moves in its order.
	c.advance(31 * time.Second)
	if done := waitMove(t, s, "dlq.fifo"); done.Status != contracts.MoveTaskCompleted ||
		done.Moved != 3 {
		t.Fatalf("ended task = %+v, want all 3 moved", done)
	}
	requireBodies(t, receive(t, s, "dest.fifo", 10), "free", "held-1", "held-2")
}

// waitMoved waits for the latest task of source to have moved n messages.
func waitMoved(t *testing.T, s *Service, source string, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		tasks, err := s.ListMoveTasks(t.Context(), source, 1)
		if err != nil || len(tasks) != 1 {
			t.Fatalf("list the tasks of %s = %v, %v", source, tasks, err)
		}
		if tasks[0].Moved == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the task on %s did not move %d: %+v", source, n, tasks[0])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
