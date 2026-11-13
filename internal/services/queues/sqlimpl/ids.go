package sqlimpl

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// newMessageID returns a message ID in the form SQS's take: a random
// (version 4) UUID.
func newMessageID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to generate a message ID: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}

// nonceLength is the bytes of randomness that tell one receive from another.
const nonceLength = 16

// newNonce returns the nonce of a receive, as the store keeps it.
func newNonce() (string, error) {
	var b [nonceLength]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to generate a receipt: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// receiptVersion starts every receipt handle, so the format can change.
const receiptVersion = 1

// receipt is what a receipt handle carries: the queue and the message row
// of a receive, and the receive's nonce.
type receipt struct {
	nonce   string
	queueID int64
	rowID   int64
}

// encode returns the receipt handle: opaque base64, as SQS's are.
func (r receipt) encode() string {
	nonce, err := hex.DecodeString(r.nonce)
	if err != nil || len(nonce) != nonceLength {
		// The store makes every nonce; one of another form is a bug.
		panic(fmt.Sprintf("sqlimpl: malformed stored nonce %q", r.nonce))
	}
	b := make([]byte, 0, 1+8+8+nonceLength)
	b = append(b, receiptVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(r.queueID)) //nolint:gosec // IDs are positive
	b = binary.BigEndian.AppendUint64(b, uint64(r.rowID))   //nolint:gosec // IDs are positive
	b = append(b, nonce...)
	return base64.StdEncoding.EncodeToString(b)
}

// parseReceipt reads a receipt handle, reporting false for one this store
// did not issue.
func parseReceipt(handle string) (receipt, bool) {
	b, err := base64.StdEncoding.DecodeString(handle)
	if err != nil || len(b) != 1+8+8+nonceLength || b[0] != receiptVersion {
		return receipt{}, false
	}
	return receipt{
		queueID: int64(binary.BigEndian.Uint64(b[1:9])),  //nolint:gosec // checked below
		rowID:   int64(binary.BigEndian.Uint64(b[9:17])), //nolint:gosec // checked below
		nonce:   hex.EncodeToString(b[17:]),
	}, int64(binary.BigEndian.Uint64(b[1:9])) > 0 //nolint:gosec // a negative ID names nothing
}

// sequenceNumber formats a FIFO queue's sequence number: a decimal string
// of 20 digits, so the numbers of a queue also sort as strings.
func sequenceNumber(n int64) string {
	return fmt.Sprintf("%020d", n)
}
