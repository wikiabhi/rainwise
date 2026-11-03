package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type eventStore struct{ db *sql.DB }

func (s eventStore) InsertEvent(ctx context.Context, e domain.StoredEvent) (bool, error) {
	res, err := s.db.ExecContext(ctx, `INSERT INTO events (id, dedup_key, payload, status, attempts, next_attempt, received_at)
		VALUES (?,?,?,?,?,?,?) ON CONFLICT(dedup_key) DO NOTHING`,
		e.ID, e.DedupKey, toJSON(e.Event), e.Status, e.Attempts, fmtTime(e.NextAttempt), fmtTime(e.ReceivedAt))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s eventStore) ClaimEvents(ctx context.Context, now time.Time, limit int) ([]domain.StoredEvent, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id, dedup_key, payload, status, attempts, next_attempt, received_at FROM events
		WHERE status = 'pending' AND claimed = 0 AND next_attempt <= ?
		ORDER BY next_attempt, received_at, rowid LIMIT ?`, fmtTime(now), limit)
	if err != nil {
		return nil, err
	}
	var out []domain.StoredEvent
	for rows.Next() {
		var (
			e              domain.StoredEvent
			payload, na, r string
		)
		if err := rows.Scan(&e.ID, &e.DedupKey, &payload, &e.Status, &e.Attempts, &na, &r); err != nil {
			rows.Close()
			return nil, err
		}
		if err := fromJSON(payload, &e.Event); err != nil {
			rows.Close()
			return nil, err
		}
		if e.NextAttempt, err = parseTime(na); err != nil {
			rows.Close()
			return nil, err
		}
		if e.ReceivedAt, err = parseTime(r); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, e := range out {
		if _, err := tx.ExecContext(ctx, `UPDATE events SET claimed = 1 WHERE id = ?`, e.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s eventStore) UpdateEvent(ctx context.Context, e domain.StoredEvent) error {
	res, err := s.db.ExecContext(ctx, `UPDATE events SET dedup_key=?, payload=?, status=?, attempts=?, next_attempt=?, received_at=?, claimed=0
		WHERE id = ?`, e.DedupKey, toJSON(e.Event), e.Status, e.Attempts, fmtTime(e.NextAttempt), fmtTime(e.ReceivedAt), e.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return notFound("event", e.ID)
	}
	return nil
}

func (s eventStore) DropHeldBefore(ctx context.Context, t time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM events WHERE status = 'held' AND received_at < ?`, fmtTime(t))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}
