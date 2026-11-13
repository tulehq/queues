//go:build e2e

package e2e

import (
	"bytes"
	"crypto/md5" //nolint:gosec // G501: SQS's digests are MD5, which the tests recompute
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// md5Hex is the hex MD5 of s, the MD5OfMessageBody SQS answers.
func md5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec // G401: the digest SQS defines
	return hex.EncodeToString(sum[:])
}

// attrsMD5 is the digest of message attributes as the SQS Developer Guide
// defines it (Message metadata, Calculating the MD5 message digest for
// message attributes): the attributes sorted by name; for each, the name and
// the data type, each as a 4-byte big-endian length and its UTF-8 bytes;
// the transport type, 1 for String and Number, 2 for Binary; and the value,
// as a 4-byte length and its bytes. The custom type suffix is part of the
// data type. "" for no attributes, as SQS sends no digest then.
//
// The Go SDK checks only the body's digest (cust_checksum_validation.go),
// not this one, so the tests check it themselves.
func attrsMD5(attrs map[string]types.MessageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}
	var buf bytes.Buffer
	for _, name := range slices.Sorted(mapKeys(attrs)) {
		v := attrs[name]
		writeLen(&buf, []byte(name))
		dataType := aws.ToString(v.DataType)
		writeLen(&buf, []byte(dataType))
		if strings.HasPrefix(dataType, "Binary") {
			buf.WriteByte(2)
			writeLen(&buf, v.BinaryValue)
		} else {
			buf.WriteByte(1)
			writeLen(&buf, []byte(aws.ToString(v.StringValue)))
		}
	}
	sum := md5.Sum(buf.Bytes()) //nolint:gosec // G401: the digest SQS defines
	return hex.EncodeToString(sum[:])
}

// systemAttrsMD5 is the digest of message system attributes, which are
// "structured exactly like message attributes" (Developer Guide, Amazon SQS
// message system attributes), so the same encoding applies.
func systemAttrsMD5(attrs map[string]types.MessageSystemAttributeValue) string {
	conv := make(map[string]types.MessageAttributeValue, len(attrs))
	for k, v := range attrs {
		conv[k] = types.MessageAttributeValue{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return attrsMD5(conv)
}

func writeLen(buf *bytes.Buffer, b []byte) {
	//nolint:gosec // G115: small test values
	_ = binary.Write(buf, binary.BigEndian, uint32(len(b)))
	buf.Write(b)
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func stringAttr(dataType, value string) types.MessageAttributeValue {
	return types.MessageAttributeValue{
		DataType:    aws.String(dataType),
		StringValue: aws.String(value),
	}
}

func binaryAttr(dataType string, value []byte) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String(dataType), BinaryValue: value}
}

// TestAttributeDigestMatchesAWSExample checks the suite's own digest against
// the SendMessage API reference example: the body "This is a test message"
// and two String attributes answer MD5OfMessageBody
// fafb00f5732ab283681e124bf8747ed1 and MD5OfMessageAttributes
// c48838208d2b4e14e3ca0093a8443f09. It needs no server; it keeps the other
// tests honest.
func TestAttributeDigestMatchesAWSExample(t *testing.T) {
	requireEqual(t, md5Hex("This is a test message"), "fafb00f5732ab283681e124bf8747ed1",
		"body digest of the AWS example")
	got := attrsMD5(map[string]types.MessageAttributeValue{
		"my_attribute_name_1": stringAttr("String", "my_attribute_value_1"),
		"my_attribute_name_2": stringAttr("String", "my_attribute_value_2"),
	})
	requireEqual(t, got, "c48838208d2b4e14e3ca0093a8443f09", "attribute digest of the AWS example")
}
