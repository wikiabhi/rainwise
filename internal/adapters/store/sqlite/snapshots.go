package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type snapStore struct{ db *sql.DB }

func (s snapStore) CreateSnapshot(ctx context.Context, sn domain.Snapshot) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO snapshots (id, provider, job_id, created_at, collections, created_collections, scope_all, first_backfill_done)
		VALUES (?,?,?,?,?,?,?,?)`, sn.ID, sn.Provider, sn.JobID, fmtTime(sn.CreatedAt), toJSON(sn.Collections),
		toJSON(sn.CreatedCollections), boolInt(sn.ScopeAll), sn.FirstBackfillDone)
	if isUnique(err) {
		return fmt.Errorf("snapshot %q exists: %w", sn.ID, domain.ErrConflict)
	}
	return err
}

func (s snapStore) exists(ctx context.Context, id string) error {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM snapshots WHERE id = ?`, id).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return notFound("snapshot", id)
	}
	return nil
}

func (s snapStore) AddSnapshotItems(ctx context.Context, id string, items []domain.SnapshotItem) error {
	if err := s.exists(ctx, id); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, it := range items {
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_items (snapshot_id, bookmark, collection, tags, pin_source, pinned_at) VALUES (?,?,?,?,?,?)`,
			id, it.Bookmark.String(), idString(it.Collection), toJSON(it.Tags), string(it.PinSource), fmtTimePtr(it.PinnedAt)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s snapStore) AddCreatedCollection(ctx context.Context, id string, c domain.ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT created_collections FROM snapshots WHERE id = ?`, id).Scan(&raw); err != nil {
		return wrapNoRows(err, "snapshot", id)
	}
	var list []domain.ID
	if err := fromJSON(raw, &list); err != nil {
		return err
	}
	list = append(list, c)
	if _, err := tx.ExecContext(ctx, `UPDATE snapshots SET created_collections = ? WHERE id = ?`, toJSON(list), id); err != nil {
		return err
	}
	return tx.Commit()
}

const snapCols = `id, provider, job_id, created_at, collections, created_collections, scope_all, first_backfill_done`

func scanSnapshot(r scanner) (domain.Snapshot, error) {
	var (
		sn                  domain.Snapshot
		created, cols, made string
		all                 int
	)
	if err := r.Scan(&sn.ID, &sn.Provider, &sn.JobID, &created, &cols, &made, &all, &sn.FirstBackfillDone); err != nil {
		return domain.Snapshot{}, err
	}
	var err error
	if sn.CreatedAt, err = parseTime(created); err != nil {
		return domain.Snapshot{}, err
	}
	if err := fromJSON(cols, &sn.Collections); err != nil {
		return domain.Snapshot{}, err
	}
	if err := fromJSON(made, &sn.CreatedCollections); err != nil {
		return domain.Snapshot{}, err
	}
	sn.ScopeAll = all != 0
	return sn, nil
}

func (s snapStore) GetSnapshot(ctx context.Context, id string) (domain.Snapshot, error) {
	sn, err := scanSnapshot(s.db.QueryRowContext(ctx, `SELECT `+snapCols+` FROM snapshots WHERE id = ?`, id))
	if err != nil {
		return domain.Snapshot{}, wrapNoRows(err, "snapshot", id)
	}
	return sn, nil
}

func (s snapStore) ListSnapshots(ctx context.Context, cursor string, limit int) (domain.Page[domain.Snapshot], error) {
	off, lim, err := pageArgs(cursor, limit)
	if err != nil {
		return domain.Page[domain.Snapshot]{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+snapCols+` FROM snapshots ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`, lim+1, off)
	if err != nil {
		return domain.Page[domain.Snapshot]{}, err
	}
	defer rows.Close()
	var out []domain.Snapshot
	for rows.Next() {
		sn, err := scanSnapshot(rows)
		if err != nil {
			return domain.Page[domain.Snapshot]{}, err
		}
		out = append(out, sn)
	}
	if err := rows.Err(); err != nil {
		return domain.Page[domain.Snapshot]{}, err
	}
	return finishPage(out, off, lim), nil
}

func (s snapStore) ListSnapshotItems(ctx context.Context, id, cursor string, limit int) (domain.Page[domain.SnapshotItem], error) {
	off, lim, err := pageArgs(cursor, limit)
	if err != nil {
		return domain.Page[domain.SnapshotItem]{}, err
	}
	if err := s.exists(ctx, id); err != nil {
		return domain.Page[domain.SnapshotItem]{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT bookmark, collection, tags, pin_source, pinned_at FROM snapshot_items WHERE snapshot_id = ? ORDER BY seq LIMIT ? OFFSET ?`,
		id, lim+1, off)
	if err != nil {
		return domain.Page[domain.SnapshotItem]{}, err
	}
	defer rows.Close()
	var out []domain.SnapshotItem
	for rows.Next() {
		var b, c, tags, pin string
		var pinned sql.NullString
		if err := rows.Scan(&b, &c, &tags, &pin, &pinned); err != nil {
			return domain.Page[domain.SnapshotItem]{}, err
		}
		it := domain.SnapshotItem{PinSource: domain.PinSource(pin)}
		if it.PinnedAt, err = parseTimePtr(pinned); err != nil {
			return domain.Page[domain.SnapshotItem]{}, err
		}
		if it.Bookmark, err = parseID(b); err != nil {
			return domain.Page[domain.SnapshotItem]{}, err
		}
		if it.Collection, err = parseID(c); err != nil {
			return domain.Page[domain.SnapshotItem]{}, err
		}
		if err := fromJSON(tags, &it.Tags); err != nil {
			return domain.Page[domain.SnapshotItem]{}, err
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return domain.Page[domain.SnapshotItem]{}, err
	}
	return finishPage(out, off, lim), nil
}

func (s snapStore) PruneSnapshots(ctx context.Context, keep int, protect []string) error {
	if keep < 0 {
		keep = 0
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM snapshots ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	prot := map[string]bool{}
	for _, p := range protect {
		prot[p] = true
	}
	for i, id := range ids {
		if i < keep || prot[id] {
			continue
		}
		// snapshot_items go with it via ON DELETE CASCADE.
		if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE id = ?`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
