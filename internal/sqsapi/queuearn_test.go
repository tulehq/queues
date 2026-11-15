package sqsapi

import "testing"

func TestParseQueueARNSplitsAnARNInForm(t *testing.T) {
	partition, region, account, name, err := ParseQueueARN(
		"arn:aws-cn:sqs:cn-north-1:000000000000:orders.fifo")
	if err != nil || partition != "aws-cn" || region != "cn-north-1" ||
		account != "000000000000" || name != "orders.fifo" {
		t.Fatalf("got %q %q %q %q, %v", partition, region, account, name, err)
	}
}

func TestParseQueueARNLeavesTheNameToCheckQueueName(t *testing.T) {
	// The name is everything after the fifth colon.
	if _, _, _, name, err := ParseQueueARN("arn:aws:sqs:us-east-1:0:a:b"); err != nil ||
		name != "a:b" {
		t.Fatalf("a name with a colon: %q, %v", name, err)
	}
}

func TestParseQueueARNRefusesAStringNotInForm(t *testing.T) {
	for _, arn := range []string{
		"", "orders", "arn:aws:sqs:us-east-1:000000000000",
		"arn:aws:sns:us-east-1:000000000000:orders", "urn:aws:sqs:us-east-1:000000000000:orders",
		"arn::sqs:us-east-1:000000000000:orders", "arn:aws:sqs::000000000000:orders",
		"arn:aws:sqs:us-east-1::orders", "arn:aws:sqs:us-east-1:000000000000:",
	} {
		if _, _, _, _, err := ParseQueueARN(arn); err == nil {
			t.Errorf("%q parsed", arn)
		}
		if CheckQueueARN("QueueArn", arn) == nil {
			t.Errorf("%q passed CheckQueueARN", arn)
		}
	}
}
