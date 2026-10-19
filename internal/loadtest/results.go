package loadtest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Latency is request-to-ACCEPTED for a set of deliveries, from stored timestamps.
type Latency struct {
	Count              int
	P50, P95, P99, Max float64 // seconds
}

// Results is what Postgres says about the deliveries of one phase of one run.
type Results struct {
	Prefix string
	// Total deliveries stored, and how many of them have an accepted_at.
	Total, Accepted int
	ByState         map[string]int
	First, Retried  Latency
	// Pending is how many were still PENDING when the drain wait ended.
	Pending int
	// Drain is how long the wait for PENDING to reach zero took.
	Drain time.Duration
	// SQL is each query run with its raw answer, for the record.
	SQL []string
}

// Reader reads results from the database.
type Reader struct {
	Pool      *pgxpool.Pool
	Primaries []string
}

// WaitDrained waits until no delivery of the prefix is PENDING, or timeout passes.
func (r *Reader) WaitDrained(ctx context.Context, prefix string, timeout time.Duration) (time.Duration, int, error) {
	start := time.Now()
	for {
		var pending int
		err := r.Pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE recipient_id LIKE $1 AND state = 'PENDING'`, prefix+"%").Scan(&pending)
		if err != nil {
			return 0, 0, err
		}
		if pending == 0 || time.Since(start) > timeout {
			return time.Since(start), pending, nil
		}
		select {
		case <-ctx.Done():
			return time.Since(start), pending, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

const latencyQuery = `
SELECT first, count(*),
       percentile_cont(0.50) WITHIN GROUP (ORDER BY lat),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat),
       percentile_cont(0.99) WITHIN GROUP (ORDER BY lat),
       max(lat)
FROM (
  SELECT extract(epoch FROM d.accepted_at - d.created_at) AS lat,
         (i.retries = 0 AND i.provider = ANY($2)) AS first
  FROM deliveries d JOIN inbox i ON i.delivery_id = d.id
  WHERE d.recipient_id LIKE $1 AND d.accepted_at IS NOT NULL
) t GROUP BY first`

const stateQuery = `
SELECT state, count(*), count(accepted_at) FROM deliveries WHERE recipient_id LIKE $1 GROUP BY state ORDER BY state`

// Read reads the results of the deliveries whose recipient id starts with prefix.
func (r *Reader) Read(ctx context.Context, prefix string) (Results, error) {
	res := Results{Prefix: prefix, ByState: map[string]int{}}
	like := prefix + "%"

	rows, err := r.Pool.Query(ctx, stateQuery, like)
	if err != nil {
		return res, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "-- states\n%s\n-- $1 = %q\n", strings.TrimSpace(stateQuery), like)
	for rows.Next() {
		var state string
		var n, acc int
		if err := rows.Scan(&state, &n, &acc); err != nil {
			rows.Close()
			return res, err
		}
		res.ByState[state], res.Total, res.Accepted = n, res.Total+n, res.Accepted+acc
		fmt.Fprintf(&b, "%-12s %8d deliveries, %8d with accepted_at\n", state, n, acc)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}
	res.SQL = append(res.SQL, b.String())

	rows, err = r.Pool.Query(ctx, latencyQuery, like, r.Primaries)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	b.Reset()
	fmt.Fprintf(&b, "-- request to ACCEPTED, seconds (accepted_at - created_at)\n%s\n-- $1 = %q, $2 = %v\n", strings.TrimSpace(latencyQuery), like, r.Primaries)
	for rows.Next() {
		var first bool
		var l Latency
		if err := rows.Scan(&first, &l.Count, &l.P50, &l.P95, &l.P99, &l.Max); err != nil {
			return res, err
		}
		fmt.Fprintf(&b, "first=%-5v n=%-8d p50=%.4f p95=%.4f p99=%.4f max=%.4f\n", first, l.Count, l.P50, l.P95, l.P99, l.Max)
		if first {
			res.First = l
		} else {
			res.Retried = l
		}
	}
	res.SQL = append(res.SQL, b.String())
	return res, rows.Err()
}
