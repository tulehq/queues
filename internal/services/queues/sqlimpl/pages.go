package sqlimpl

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"tule.dev/queues/internal/contracts"
)

// A page token names the listing it belongs to, the scope of that listing
// (the prefix, queue or filter it was asked for), and the cursor of the last
// item it returned. A token is only accepted back by the same listing with
// the same scope, so a token from another listing, or one this service did
// not issue, fails rather than resuming somewhere arbitrary. It is encoded
// to keep it opaque to clients.
const tokenSeparator = "\x00"

// The listings a token can belong to.
const (
	listQueues    = "queues"
	listDLQSource = "dead-letter-sources"
	listMessages  = "messages"
)

func encodeToken(list, scope, cursor string) string {
	raw := strings.Join([]string{list, scope, cursor}, tokenSeparator)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeToken returns the cursor a listing should resume after, or "" for
// an empty token.
func decodeToken(token, list, scope string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", contracts.ErrInvalidToken
	}
	parts := strings.SplitN(string(raw), tokenSeparator, 3)
	if len(parts) != 3 || parts[0] != list || parts[1] != scope || parts[2] == "" {
		return "", contracts.ErrInvalidToken
	}
	return parts[2], nil
}

// scanner is a row to scan: an *sql.Rows positioned on a row, or an
// *sql.Row.
type scanner interface {
	Scan(dest ...any) error
}

// queryAll runs query in tx and scans every row with scan.
func queryAll[T any](
	ctx context.Context,
	tx *sql.Tx,
	scan func(scanner) (T, error),
	query string,
	args ...any,
) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	// Every error of the iteration is reported by rows.Err below; Close
	// after a full iteration has nothing left to report.
	defer func() { _ = rows.Close() }()

	var out []T
	for rows.Next() {
		v, sErr := scan(rows)
		if sErr != nil {
			return nil, fmt.Errorf("scan: %w", sErr)
		}
		out = append(out, v)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// scanOne scans the row of query, reporting false when there is none.
func scanOne(ctx context.Context, tx *sql.Tx, dest []any, query string, args ...any) (
	bool, error,
) {
	err := tx.QueryRowContext(ctx, query, args...).Scan(dest...)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("query: %w", err)
	}
}

func exec(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

// scanString scans a one-column row of text.
func scanString(r scanner) (string, error) {
	var s string
	err := r.Scan(&s)
	return s, err //nolint:wrapcheck // queryAll wraps it
}
