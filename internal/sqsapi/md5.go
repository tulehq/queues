package sqsapi

import (
	"crypto/md5" //nolint:gosec // SQS's digests are MD5; they check integrity, not secrecy
	"encoding/binary"
	"encoding/hex"
	"strings"

	"tule.dev/queues/internal/contracts"
)

// MD5OfBody is the digest SQS returns for a message body: the MD5 of its
// UTF-8 bytes, in hex.
func MD5OfBody(body string) string {
	sum := md5.Sum([]byte(body)) //nolint:gosec // see the import
	return hex.EncodeToString(sum[:])
}

// The transport types of the attribute digest: String and Number travel as
// strings, Binary as bytes.
const (
	transportString = 1
	transportBinary = 2
)

// MD5OfAttributes is the digest SQS returns for message attributes, or for
// message system attributes, as the SQS developer guide gives the
// algorithm: for each attribute in ascending order of name, the name, the
// data type (custom suffix included), one transport byte (1 for String and
// Number, 2 for Binary) and the value, each length-prefixed with 4
// big-endian bytes. It returns "" for no attributes, which SQS reports
// without a digest.
func MD5OfAttributes(attrs map[string]contracts.MessageAttribute) string {
	if len(attrs) == 0 {
		return ""
	}
	h := md5.New() //nolint:gosec // see the import
	put := func(b []byte) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(b))) //nolint:gosec // values are at most 1 MiB
		h.Write(n[:])
		h.Write(b)
	}
	for _, name := range sortedKeys(attrs) {
		a := attrs[name]
		put([]byte(name))
		put([]byte(a.DataType))
		if baseType(a.DataType) == typeBinary {
			h.Write([]byte{transportBinary})
			put(a.BinaryValue)
			continue
		}
		h.Write([]byte{transportString})
		var v string
		if a.StringValue != nil {
			v = *a.StringValue
		}
		put([]byte(v))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// baseType is the logical type of a data type: what comes before a custom
// suffix ("Number" of "Number.float").
func baseType(dataType string) string {
	base, _, _ := strings.Cut(dataType, ".")
	return base
}
