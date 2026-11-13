//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Queue operations against the SDK model's documentation (api_op_*.go),
// its errors (types/errors.go) and the codes the SDK reports for them
// (schemas/schemas.go, AWSQueryError).

func TestCreateQueue(t *testing.T) {
	c, srv := newClient(t)

	t.Run("returns the queue URL", func(t *testing.T) {
		requireEqual(t, createQueue(t, c, "orders", nil), srv.queueURL("orders"), "queue URL")
	})

	t.Run("again with the same attributes returns the same URL", func(t *testing.T) {
		attrs := map[string]string{"VisibilityTimeout": "40", "DelaySeconds": "5"}
		first := createQueue(t, c, "same", attrs)
		requireEqual(t, createQueue(t, c, "same", attrs), first, "URL of the second create")
	})

	t.Run("again with different attributes is QueueNameExists", func(t *testing.T) {
		createQueue(t, c, "differs", map[string]string{"VisibilityTimeout": "40"})
		err := errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
			QueueName:  aws.String("differs"),
			Attributes: map[string]string{"VisibilityTimeout": "41"},
		}))
		requireModeled[*types.QueueNameExists](t, err, "QueueAlreadyExists")
		got := queueAttrs(t, c, srv.queueURL("differs"), "VisibilityTimeout")
		requireEqual(t, got["VisibilityTimeout"], "40", "VisibilityTimeout after the refusal")
	})

	t.Run("an 80-character name is accepted", func(t *testing.T) {
		name := strings.Repeat("a", 79) + "Z"
		requireEqual(t, createQueue(t, c, name, nil), srv.queueURL(name), "queue URL")
	})

	// The Developer Guide's example error (Interpreting Amazon SQS XML API
	// responses) answers a queue name outside the rule, "alphanumeric
	// characters, hyphens and underscores, up to 80" (CreateQueue,
	// QueueName), with InvalidParameterValue.
	for _, name := range []string{"", "has space", "dot.name", "slash/name", strings.Repeat("a", 81)} {
		t.Run(fmt.Sprintf("name %.12q is InvalidParameterValue", name), func(t *testing.T) {
			err := errOf(
				c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String(name)}),
			)
			requireInvalidParameter(t, err)
		})
	}

	t.Run("an unknown attribute is InvalidAttributeName", func(t *testing.T) {
		err := errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
			QueueName:  aws.String("unknown-attr"),
			Attributes: map[string]string{"Bogus": "1"},
		}))
		requireModeled[*types.InvalidAttributeName](t, err, "InvalidAttributeName")
		requireQueueMissing(t, c, "unknown-attr")
	})
}

func TestQueueURLAndARNFollowConfiguration(t *testing.T) {
	// The design: URLs <scheme>://<host>/<account>/<name> with the host from
	// TULE_Queues_BaseURL when set, ARNs arn:aws:sqs:<region>:<account>:<name>.
	// Assumption: BaseURL is given without a trailing slash and the path is
	// appended to it.
	srv := startServer(t, map[string]string{
		"TULE_Queues_AccountID": "111122223333",
		"TULE_Queues_Region":    "eu-west-1",
		"TULE_Queues_BaseURL":   "http://queues.example:9050",
	})
	c := clientFor(srv, func(o *sqs.Options) { o.Region = "eu-west-1" })

	u := createQueue(t, c, "configured", nil)
	requireEqual(t, u, "http://queues.example:9050/111122223333/configured", "queue URL")

	got, err := c.GetQueueUrl(
		t.Context(),
		&sqs.GetQueueUrlInput{QueueName: aws.String("configured")},
	)
	requireNoErr(t, err, "get queue URL")
	requireEqual(t, aws.ToString(got.QueueUrl), u, "GetQueueUrl")

	arn := queueAttrs(t, c, u, "QueueArn")["QueueArn"]
	requireEqual(t, arn, "arn:aws:sqs:eu-west-1:111122223333:configured", "QueueArn")
}

func TestDefaultARN(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "arned", nil)
	requireEqual(t, queueAttrs(t, c, u, "QueueArn")["QueueArn"],
		"arn:aws:sqs:us-east-1:000000000000:arned", "QueueArn")
}

func TestGetQueueUrl(t *testing.T) {
	c, srv := newClient(t)
	createQueue(t, c, "orders", nil)

	out, err := c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String("orders")})
	requireNoErr(t, err, "get queue URL")
	requireEqual(t, aws.ToString(out.QueueUrl), srv.queueURL("orders"), "queue URL")

	// "Queue URLs and names are case-sensitive" (GetQueueUrl, QueueName).
	requireQueueMissing(t, c, "Orders")
	requireQueueMissing(t, c, "nothing")
}

// requireQueueMissing fails unless GetQueueUrl answers QueueDoesNotExist
// for name.
func requireQueueMissing(t *testing.T, c *sqs.Client, name string) {
	t.Helper()
	err := errOf(c.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)}))
	requireNonExistent(t, err)
}

// requireNonExistent fails unless err is QueueDoesNotExist, whose
// Query-compatible code is AWS.SimpleQueueService.NonExistentQueue.
func requireNonExistent(t *testing.T, err error) {
	t.Helper()
	requireModeled[*types.QueueDoesNotExist](t, err, "AWS.SimpleQueueService.NonExistentQueue")
}

func TestListQueues(t *testing.T) {
	c, srv := newClient(t)

	t.Run("no queues lists none", func(t *testing.T) {
		out, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{})
		requireNoErr(t, err, "list queues")
		requireEqual(t, len(out.QueueUrls), 0, "queue count")
	})

	names := []string{"app-a", "app-b", "app-c", "billing", "App-d"}
	for _, n := range names {
		createQueue(t, c, n, nil)
	}
	urls := func(ns ...string) []string {
		out := make([]string, 0, len(ns))
		for _, n := range ns {
			out = append(out, srv.queueURL(n))
		}
		return out
	}

	t.Run("all, with no NextToken without MaxResults", func(t *testing.T) {
		out, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{})
		requireNoErr(t, err, "list queues")
		requireSameSet(t, out.QueueUrls, urls(names...), "queue URLs")
		requireEqual(t, aws.ToString(out.NextToken), "", "NextToken without MaxResults")
	})

	t.Run("by prefix, case-sensitive", func(t *testing.T) {
		out, err := c.ListQueues(
			t.Context(),
			&sqs.ListQueuesInput{QueueNamePrefix: aws.String("app-")},
		)
		requireNoErr(t, err, "list queues by prefix")
		requireSameSet(t, out.QueueUrls, urls("app-a", "app-b", "app-c"), "queue URLs")
	})

	t.Run("paginated by MaxResults", func(t *testing.T) {
		all, pages := listQueuePages(t, c, &sqs.ListQueuesInput{MaxResults: aws.Int32(2)})
		requireSameSet(t, all, urls(names...), "queue URLs over every page")
		requireEqual(t, pages, 3, "pages of 2 over 5 queues")
	})

	t.Run("prefix and pagination together", func(t *testing.T) {
		all, _ := listQueuePages(t, c, &sqs.ListQueuesInput{
			MaxResults:      aws.Int32(1),
			QueueNamePrefix: aws.String("app-"),
		})
		requireSameSet(t, all, urls("app-a", "app-b", "app-c"), "queue URLs over every page")
	})
}

// listQueuePages follows ListQueues' NextToken, checking no page holds
// more than MaxResults, and returns every URL and the page count.
func listQueuePages(t *testing.T, c *sqs.Client, in *sqs.ListQueuesInput) ([]string, int) {
	t.Helper()
	var all []string
	pages := 0
	p := sqs.NewListQueuesPaginator(c, in)
	for p.HasMorePages() {
		page, err := p.NextPage(t.Context())
		requireNoErr(t, err, "list queues page")
		if n := len(page.QueueUrls); n > int(aws.ToInt32(in.MaxResults)) {
			t.Fatalf("page of %d queues, want at most %d", n, aws.ToInt32(in.MaxResults))
		}
		all = append(all, page.QueueUrls...)
		pages++
	}
	return all, pages
}

func TestQueueAttributesDefaults(t *testing.T) {
	c, _ := newClient(t)
	before := time.Now().Add(-2 * time.Second).Unix()
	u := createQueue(t, c, "defaults", nil)
	after := time.Now().Add(2 * time.Second).Unix()

	a := queueAttrs(t, c, u, "All")
	// Defaults from CreateQueue, Attributes.
	want := map[string]string{
		"DelaySeconds":                          "0",
		"MaximumMessageSize":                    "1048576",
		"MessageRetentionPeriod":                "345600",
		"ReceiveMessageWaitTimeSeconds":         "0",
		"VisibilityTimeout":                     "30",
		"ApproximateNumberOfMessages":           "0",
		"ApproximateNumberOfMessagesNotVisible": "0",
		"ApproximateNumberOfMessagesDelayed":    "0",
		"QueueArn":                              "arn:aws:sqs:us-east-1:000000000000:defaults",
	}
	for k, v := range want {
		requireEqual(t, a[k], v, k)
	}
	// CreatedTimestamp and LastModifiedTimestamp are epoch seconds
	// (GetQueueAttributes, AttributeNames).
	for _, k := range []string{"CreatedTimestamp", "LastModifiedTimestamp"} {
		ts := int64(atoi(t, a[k], k))
		if ts < before || ts > after {
			t.Fatalf("%s = %d, want epoch seconds between %d and %d", k, ts, before, after)
		}
	}
	// Assumption: as on SQS, a standard queue does not report the
	// attributes that "apply only to FIFO queues".
	for _, k := range []string{"FifoQueue", "ContentBasedDeduplication"} {
		if v, ok := a[k]; ok {
			t.Fatalf("standard queue reports %s=%q", k, v)
		}
	}
}

func TestQueueAttributesSelection(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "selected", map[string]string{"VisibilityTimeout": "45"})

	t.Run("named attributes only", func(t *testing.T) {
		a := queueAttrs(t, c, u, "VisibilityTimeout", "QueueArn")
		requireSameSet(t, keys(a), []string{"VisibilityTimeout", "QueueArn"}, "attribute names")
		requireEqual(t, a["VisibilityTimeout"], "45", "VisibilityTimeout")
	})

	t.Run("none requested returns none", func(t *testing.T) {
		// "if you don't specify values for this parameter, the request
		// returns empty results" (GetQueueAttributes, AttributeNames).
		requireEqual(t, len(queueAttrs(t, c, u)), 0, "attribute count")
	})

	t.Run("an unknown name is InvalidAttributeName", func(t *testing.T) {
		err := errOf(c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(u),
			AttributeNames: []types.QueueAttributeName{"Bogus"},
		}))
		requireModeled[*types.InvalidAttributeName](t, err, "InvalidAttributeName")
	})

	t.Run("a missing queue is QueueDoesNotExist", func(t *testing.T) {
		err := errOf(c.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(u + "-missing"),
			AttributeNames: []types.QueueAttributeName{"All"},
		}))
		requireNonExistent(t, err)
	})
}

func TestApproximateCounts(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "counted", nil)

	send(t, c, u, "visible")
	send(t, c, u, "in flight")
	sendInput(t, c, &sqs.SendMessageInput{
		QueueUrl:     aws.String(u),
		MessageBody:  aws.String("delayed"),
		DelaySeconds: 600,
	})
	requireCounts(t, c, u, counts{visible: 2, delayed: 1}, "after sending")

	receiveOne(t, c, u)
	requireCounts(t, c, u, counts{visible: 1, notVisible: 1, delayed: 1}, "after one receive")
}

// attrRange is a queue attribute with its valid range from CreateQueue,
// Attributes.
type attrRange struct {
	name     string
	min, max int
}

var attrRanges = []attrRange{
	{name: "DelaySeconds", min: 0, max: 900},
	{name: "MaximumMessageSize", min: 1024, max: 1048576},
	{name: "MessageRetentionPeriod", min: 60, max: 1209600},
	{name: "ReceiveMessageWaitTimeSeconds", min: 0, max: 20},
	{name: "VisibilityTimeout", min: 0, max: 43200},
}

func TestAttributeRanges(t *testing.T) {
	c, _ := newClient(t)
	target := createQueue(t, c, "target", nil)

	for i, r := range attrRanges {
		t.Run(r.name+" bounds are accepted", func(t *testing.T) {
			for j, v := range []int{r.min, r.max} {
				s := strconv.Itoa(v)
				u := createQueue(
					t,
					c,
					fmt.Sprintf("bound-%d-%d", i, j),
					map[string]string{r.name: s},
				)
				requireEqual(t, queueAttrs(t, c, u, r.name)[r.name], s, r.name+" on create")
				setAttrs(t, c, target, map[string]string{r.name: s})
				requireEqual(t, queueAttrs(t, c, target, r.name)[r.name], s, r.name+" on set")
			}
		})

		// "A queue attribute value is invalid" (CreateQueue and
		// SetQueueAttributes, InvalidAttributeValue).
		for _, bad := range []string{strconv.Itoa(r.min - 1), strconv.Itoa(r.max + 1), "abc", "1.5"} {
			t.Run(r.name+"="+bad+" is InvalidAttributeValue", func(t *testing.T) {
				name := fmt.Sprintf("bad-%d", i)
				err := errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
					QueueName:  aws.String(name),
					Attributes: map[string]string{r.name: bad},
				}))
				requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
				requireQueueMissing(t, c, name)

				err = errOf(c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
					QueueUrl:   aws.String(target),
					Attributes: map[string]string{r.name: bad},
				}))
				requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
			})
		}
	}

	for _, v := range []string{"yes", "1"} {
		t.Run("FifoQueue="+v+" is InvalidAttributeValue", func(t *testing.T) {
			// "Valid values are true and false" (CreateQueue, FifoQueue).
			err := errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
				QueueName:  aws.String("bad-fifo.fifo"),
				Attributes: map[string]string{"FifoQueue": v},
			}))
			requireModeled[*types.InvalidAttributeValue](t, err, "InvalidAttributeValue")
		})
	}
}

func setAttrs(t *testing.T, c *sqs.Client, queueURL string, attrs map[string]string) {
	t.Helper()
	_, err := c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(queueURL),
		Attributes: attrs,
	})
	requireNoErr(t, err, "set queue attributes")
}

func TestSetQueueAttributes(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "settable", nil)

	setAttrs(t, c, u, map[string]string{
		"VisibilityTimeout":             "120",
		"DelaySeconds":                  "3",
		"MaximumMessageSize":            "2048",
		"MessageRetentionPeriod":        "600",
		"ReceiveMessageWaitTimeSeconds": "4",
	})
	a := queueAttrs(t, c, u, "All")
	for k, v := range map[string]string{
		"VisibilityTimeout": "120", "DelaySeconds": "3", "MaximumMessageSize": "2048",
		"MessageRetentionPeriod": "600", "ReceiveMessageWaitTimeSeconds": "4",
	} {
		requireEqual(t, a[k], v, k)
	}
	if atoi(t, a["LastModifiedTimestamp"], "LastModifiedTimestamp") <
		atoi(t, a["CreatedTimestamp"], "CreatedTimestamp") {
		t.Fatalf("LastModifiedTimestamp %s before CreatedTimestamp %s",
			a["LastModifiedTimestamp"], a["CreatedTimestamp"])
	}

	err := errOf(c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(u),
		Attributes: map[string]string{"Bogus": "1"},
	}))
	requireModeled[*types.InvalidAttributeName](t, err, "InvalidAttributeName")

	err = errOf(c.SetQueueAttributes(t.Context(), &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(u + "-missing"),
		Attributes: map[string]string{"VisibilityTimeout": "1"},
	}))
	requireNonExistent(t, err)
}

func TestDeleteQueue(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "doomed", nil)
	send(t, c, u, "lost")
	keep := createQueue(t, c, "kept", nil)

	_, err := c.DeleteQueue(t.Context(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "delete queue")

	// Assumption: the queue is gone at once. SQS says requests "during the
	// 60 seconds might succeed" (DeleteQueue), which permits either.
	requireQueueMissing(t, c, "doomed")
	requireNonExistent(t, errOf(c.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(u), MessageBody: aws.String("after"),
	})))
	requireNonExistent(
		t,
		errOf(c.DeleteQueue(t.Context(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})),
	)

	out, err := c.ListQueues(t.Context(), &sqs.ListQueuesInput{})
	requireNoErr(t, err, "list queues")
	requireSlice(t, out.QueueUrls, []string{keep}, "queues after the delete")

	// "You must wait 60 seconds after deleting a queue before you can create
	// another queue with the same name" (QueueDeletedRecently).
	err = errOf(c.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("doomed")}))
	requireModeled[*types.QueueDeletedRecently](
		t,
		err,
		"AWS.SimpleQueueService.QueueDeletedRecently",
	)
}

func TestQueueDeletedRecentlyEnds(t *testing.T) {
	srv, clock := startServerWithClock(t)
	c := clientFor(srv)
	u := createQueue(t, c, "reborn", nil)
	_, err := c.DeleteQueue(t.Context(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "delete queue")

	clock.Advance(t, 61*time.Second)
	requireEqual(t, createQueue(t, c, "reborn", nil), u, "URL of the queue created again")
	requireCounts(t, c, u, counts{}, "the new queue holds nothing")
}

func TestPurgeQueue(t *testing.T) {
	c, _ := newClient(t)
	u := createQueue(t, c, "purged", nil)
	for i := range 3 {
		send(t, c, u, "m"+strconv.Itoa(i))
	}
	receiveOne(t, c, u)

	// "Deletes available messages in a queue (including in-flight
	// messages)" (PurgeQueue).
	_, err := c.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "purge queue")
	requireCounts(t, c, u, counts{}, "after the purge")
	requireEmpty(t, c, u, "receive after the purge")

	// A purge within 60 seconds of the last (PurgeQueueInProgress).
	err = errOf(c.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(u)}))
	requireModeled[*types.PurgeQueueInProgress](
		t,
		err,
		"AWS.SimpleQueueService.PurgeQueueInProgress",
	)

	err = errOf(
		c.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(u + "-missing")}),
	)
	requireNonExistent(t, err)

	// The queue still takes messages.
	send(t, c, u, "after")
	requireEqual(t, aws.ToString(receiveOne(t, c, u).Body), "after", "body sent after the purge")
}

func TestPurgeQueueInProgressEnds(t *testing.T) {
	srv, clock := startServerWithClock(t)
	c := clientFor(srv)
	u := createQueue(t, c, "repurged", nil)
	_, err := c.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "first purge")

	clock.Advance(t, 61*time.Second)
	_, err = c.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(u)})
	requireNoErr(t, err, "purge 61 seconds later")
}

func TestQueueTags(t *testing.T) {
	c, _ := newClient(t)
	out, err := c.CreateQueue(t.Context(), &sqs.CreateQueueInput{
		QueueName: aws.String("tagged"),
		Tags:      map[string]string{"env": "dev", "team": "core"},
	})
	requireNoErr(t, err, "create queue with tags")
	u := aws.ToString(out.QueueUrl)

	requireTags(t, c, u, map[string]string{"env": "dev", "team": "core"}, "tags on create")

	// "A new tag with a key identical to that of an existing tag overwrites
	// the existing tag" (TagQueue); "Tags are case-sensitive".
	_, err = c.TagQueue(t.Context(), &sqs.TagQueueInput{
		QueueUrl: aws.String(u),
		Tags:     map[string]string{"env": "prod", "Env": "other", "cost": "42"},
	})
	requireNoErr(t, err, "tag queue")
	requireTags(t, c, u,
		map[string]string{"env": "prod", "Env": "other", "team": "core", "cost": "42"},
		"tags after TagQueue")

	_, err = c.UntagQueue(t.Context(), &sqs.UntagQueueInput{
		QueueUrl: aws.String(u),
		TagKeys:  []string{"team", "Env"},
	})
	requireNoErr(t, err, "untag queue")
	requireTags(t, c, u, map[string]string{"env": "prod", "cost": "42"}, "tags after UntagQueue")

	missing := aws.String(u + "-missing")
	requireNonExistent(
		t,
		errOf(c.ListQueueTags(t.Context(), &sqs.ListQueueTagsInput{QueueUrl: missing})),
	)
	requireNonExistent(t, errOf(c.TagQueue(t.Context(), &sqs.TagQueueInput{
		QueueUrl: missing, Tags: map[string]string{"a": "b"},
	})))
	requireNonExistent(t, errOf(c.UntagQueue(t.Context(), &sqs.UntagQueueInput{
		QueueUrl: missing, TagKeys: []string{"a"},
	})))
}

func requireTags(
	t *testing.T,
	c *sqs.Client,
	queueURL string,
	want map[string]string,
	what string,
) {
	t.Helper()
	out, err := c.ListQueueTags(
		t.Context(),
		&sqs.ListQueueTagsInput{QueueUrl: aws.String(queueURL)},
	)
	requireNoErr(t, err, "list queue tags")
	if len(out.Tags) != len(want) {
		t.Fatalf("%s: got %v, want %v", what, out.Tags, want)
	}
	for k, v := range want {
		if out.Tags[k] != v {
			t.Fatalf("%s: got %v, want %v", what, out.Tags, want)
		}
	}
}

// redrivePolicy is a RedrivePolicy value (CreateQueue, RedrivePolicy).
func redrivePolicy(dlqARN string, maxReceiveCount int) string {
	b, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     maxReceiveCount,
	})
	return string(b)
}

func TestRedrivePolicyRoundTrip(t *testing.T) {
	c, _ := newClient(t)
	dlq := createQueue(t, c, "dlq", nil)
	dlqARN := queueAttrs(t, c, dlq, "QueueArn")["QueueArn"]
	src := createQueue(t, c, "src", map[string]string{"RedrivePolicy": redrivePolicy(dlqARN, 3)})

	raw := queueAttrs(t, c, src, "RedrivePolicy")["RedrivePolicy"]
	var got struct {
		Target string          `json:"deadLetterTargetArn"`
		Max    json.RawMessage `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("RedrivePolicy %q is not JSON: %v", raw, err)
	}
	requireEqual(t, got.Target, dlqARN, "deadLetterTargetArn")
	// Assumption: the count may come back as a number or a string, as SQS
	// accepts both; its value is what matters.
	requireEqual(t, strings.Trim(string(got.Max), `"`), "3", "maxReceiveCount")
}

func TestListDeadLetterSourceQueues(t *testing.T) {
	c, _ := newClient(t)
	dlq := createQueue(t, c, "dlq", nil)
	dlqARN := queueAttrs(t, c, dlq, "QueueArn")["QueueArn"]
	policy := map[string]string{"RedrivePolicy": redrivePolicy(dlqARN, 2)}
	a := createQueue(t, c, "source-a", policy)
	b := createQueue(t, c, "source-b", nil)
	setAttrs(t, c, b, policy)
	createQueue(t, c, "unrelated", nil)

	out, err := c.ListDeadLetterSourceQueues(t.Context(), &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: aws.String(dlq),
	})
	requireNoErr(t, err, "list dead-letter source queues")
	requireSameSet(t, out.QueueUrls, []string{a, b}, "source queues")

	var paged []string
	p := sqs.NewListDeadLetterSourceQueuesPaginator(c, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl:   aws.String(dlq),
		MaxResults: aws.Int32(1),
	})
	for p.HasMorePages() {
		page, perr := p.NextPage(t.Context())
		requireNoErr(t, perr, "list dead-letter source queues page")
		requireEqual(t, len(page.QueueUrls) <= 1, true, "page size at most 1")
		paged = append(paged, page.QueueUrls...)
	}
	requireSameSet(t, paged, []string{a, b}, "source queues over every page")

	err = errOf(c.ListDeadLetterSourceQueues(t.Context(), &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: aws.String(dlq + "-missing"),
	}))
	requireNonExistent(t, err)
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// arnPattern is an SQS queue ARN.
var arnPattern = regexp.MustCompile(`^arn:aws:sqs:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]{1,80}(\.fifo)?$`)
