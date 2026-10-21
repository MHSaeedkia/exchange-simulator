// Package postgres is the only place that knows SQL. In kafka mode there are no subscribe
// messages, so the markets to send come from exchange_markets instead.
//
// A thin adapter with no branching of its own, so it is not unit-tested.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// marketsQuery lists one exchange's market keys in a given status. For okx the key is
// the instId itself (BTC-USDT), which is what the frames carry.
const marketsQuery = `SELECT market FROM exchange_markets
        WHERE exchange_id = $1 AND status = $2
        ORDER BY market`

// Repository owns the connection pool.
type Repository struct{ pool *pgxpool.Pool }

// New opens a pool and verifies it, so a bad DATABASE_URL fails at startup.
func New(ctx context.Context, databaseURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Repository{pool: pool}, nil
}

// Close releases the pool.
func (r *Repository) Close() { r.pool.Close() }

// Markets lists the market keys of exchangeID whose status is status.
func (r *Repository) Markets(ctx context.Context, exchangeID int64, status string) ([]string, error) {
	rows, err := r.pool.Query(ctx, marketsQuery, exchangeID, status)
	if err != nil {
		return nil, fmt.Errorf("query exchange_markets: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, fmt.Errorf("scan exchange_markets: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
