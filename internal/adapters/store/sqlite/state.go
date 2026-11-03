package sqlite

import (
	"context"
	"database/sql"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type stateStore struct{ db *sql.DB }

func (s stateStore) GetState(ctx context.Context, id domain.ID) (domain.BookmarkState, bool, error) {
	var (
		st                     domain.BookmarkState
		known, tags, ptags     string
		pin                    string
		pinned, proc, restored sql.NullString
		pending                sql.NullString
	)
	err := s.db.QueryRowContext(ctx, `SELECT known_collection, known_tags, pin_source, pinned_at, input_hash, taxonomy_version,
		model, status, pending_collection, pending_tags, fetch_attempts, processed_at, restored_at
		FROM bookmark_state WHERE id = ?`, id.String()).Scan(&known, &tags, &pin, &pinned, &st.InputHash, &st.TaxonomyVersion,
		&st.Model, &st.Status, &pending, &ptags, &st.FetchAttempts, &proc, &restored)
	if err == sql.ErrNoRows {
		return domain.BookmarkState{}, false, nil
	}
	if err != nil {
		return domain.BookmarkState{}, false, err
	}
	st.ID, st.PinSource = id, domain.PinSource(pin)
	if st.KnownCollection, err = parseID(known); err != nil {
		return domain.BookmarkState{}, false, err
	}
	if err := fromJSON(tags, &st.KnownTags); err != nil {
		return domain.BookmarkState{}, false, err
	}
	if err := fromJSON(ptags, &st.PendingTags); err != nil {
		return domain.BookmarkState{}, false, err
	}
	if pending.Valid {
		pc, err := parseID(pending.String)
		if err != nil {
			return domain.BookmarkState{}, false, err
		}
		st.PendingCollection = &pc
	}
	if st.PinnedAt, err = parseTimePtr(pinned); err != nil {
		return domain.BookmarkState{}, false, err
	}
	if st.ProcessedAt, err = parseTimePtr(proc); err != nil {
		return domain.BookmarkState{}, false, err
	}
	if st.RestoredAt, err = parseTimePtr(restored); err != nil {
		return domain.BookmarkState{}, false, err
	}
	return st, true, nil
}

// parseID decodes a stored ID; the zero ID is stored as the empty string.
func parseID(s string) (domain.ID, error) {
	if s == "" {
		return domain.ID{}, nil
	}
	return domain.ParseID(s)
}

func idString(id domain.ID) string {
	if id.IsZero() {
		return ""
	}
	return id.String()
}

func (s stateStore) PutState(ctx context.Context, st domain.BookmarkState) error {
	var pending any
	if st.PendingCollection != nil {
		pending = st.PendingCollection.String()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO bookmark_state (id, known_collection, known_tags, pin_source, pinned_at, input_hash,
		taxonomy_version, model, status, pending_collection, pending_tags, fetch_attempts, processed_at, restored_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET known_collection=excluded.known_collection, known_tags=excluded.known_tags,
		pin_source=excluded.pin_source, pinned_at=excluded.pinned_at, input_hash=excluded.input_hash,
		taxonomy_version=excluded.taxonomy_version, model=excluded.model, status=excluded.status,
		pending_collection=excluded.pending_collection, pending_tags=excluded.pending_tags,
		fetch_attempts=excluded.fetch_attempts, processed_at=excluded.processed_at, restored_at=excluded.restored_at`,
		st.ID.String(), idString(st.KnownCollection), toJSON(st.KnownTags), string(st.PinSource), fmtTimePtr(st.PinnedAt),
		st.InputHash, st.TaxonomyVersion, st.Model, st.Status, pending, toJSON(st.PendingTags), st.FetchAttempts,
		fmtTimePtr(st.ProcessedAt), fmtTimePtr(st.RestoredAt))
	return err
}

func (s stateStore) GetSetting(ctx context.Context, key string) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	return v, err == nil, err
}

func (s stateStore) PutSetting(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}
