package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type taxoStore struct{ db *sql.DB }

type queryer interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

func loadTaxonomy(ctx context.Context, q queryer, id string) (domain.Taxonomy, error) {
	var body string
	var version int
	if err := q.QueryRowContext(ctx, `SELECT version, body FROM taxonomies WHERE id = ?`, id).Scan(&version, &body); err != nil {
		return domain.Taxonomy{}, wrapNoRows(err, "taxonomy", id)
	}
	var t domain.Taxonomy
	if err := fromJSON(body, &t); err != nil {
		return domain.Taxonomy{}, err
	}
	t.Version = version
	return t, nil
}

func (s taxoStore) GetTaxonomy(ctx context.Context, id string) (domain.Taxonomy, error) {
	return loadTaxonomy(ctx, s.db, id)
}

func (s taxoStore) ListTaxonomies(ctx context.Context) ([]domain.Taxonomy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version, body FROM taxonomies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Taxonomy
	for rows.Next() {
		var version int
		var body string
		if err := rows.Scan(&version, &body); err != nil {
			return nil, err
		}
		var t domain.Taxonomy
		if err := fromJSON(body, &t); err != nil {
			return nil, err
		}
		t.Version = version
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s taxoStore) PutTaxonomy(ctx context.Context, t domain.Taxonomy, ifVersion *int) (domain.Taxonomy, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Taxonomy{}, err
	}
	defer tx.Rollback()
	cur := 0
	if err := tx.QueryRowContext(ctx, `SELECT version FROM taxonomies WHERE id = ?`, t.ID).Scan(&cur); err != nil && err != sql.ErrNoRows {
		return domain.Taxonomy{}, err
	}
	if ifVersion != nil && *ifVersion != cur {
		return domain.Taxonomy{}, fmt.Errorf("taxonomy %q is at version %d, not %d: %w", t.ID, cur, *ifVersion, domain.ErrConflict)
	}
	t.Version = cur + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO taxonomies (id, version, status, body, proposals) VALUES (?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET version=excluded.version, status=excluded.status, body=excluded.body, proposals=excluded.proposals`,
		t.ID, t.Version, t.Status, toJSON(t), toJSON(t.Proposals)); err != nil {
		return domain.Taxonomy{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Taxonomy{}, err
	}
	return t, nil
}

func (s taxoStore) SeedTaxonomy(ctx context.Context, t domain.Taxonomy) (bool, error) {
	if t.Version < 1 {
		t.Version = 1
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO taxonomies (id, version, status, body, proposals) VALUES (?,?,?,?,?)
		ON CONFLICT(id) DO NOTHING`, t.ID, t.Version, t.Status, toJSON(t), toJSON(t.Proposals))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s taxoStore) RecordProposal(ctx context.Context, id string, p domain.CollectionProposal) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t, err := loadTaxonomy(ctx, tx, id)
	if err != nil {
		return err
	}
	t.Proposals = domain.MergeProposal(t.Proposals, p)
	if _, err := tx.ExecContext(ctx, `UPDATE taxonomies SET body = ?, proposals = ? WHERE id = ?`,
		toJSON(t), toJSON(t.Proposals), id); err != nil {
		return err
	}
	return tx.Commit()
}
