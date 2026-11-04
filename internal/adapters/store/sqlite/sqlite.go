// Package sqlite is the durable ports.Store backed by modernc.org/sqlite.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	_ "modernc.org/sqlite" // CGO-free driver

	"github.com/wikiabhi/rainwise/internal/domain"
	"github.com/wikiabhi/rainwise/internal/ports"
)

const defaultLimit = 50

// Store implements ports.Store on a single SQLite database.
type Store struct {
	db *sql.DB
}

var _ ports.Store = (*Store)(nil)

// Open opens (creating if needed) the database at path and applies migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "synchronous(NORMAL)")
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Opaque: path, RawQuery: q.Encode()}).String())
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One connection serializes all access, so writers never see SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Jobs() ports.JobStore             { return jobStore{s.db} }
func (s *Store) Events() ports.EventStore         { return eventStore{s.db} }
func (s *Store) BookmarkStates() ports.StateStore { return stateStore{s.db} }
func (s *Store) Snapshots() ports.SnapshotStore   { return snapStore{s.db} }
func (s *Store) Taxonomies() ports.TaxonomyStore  { return taxoStore{s.db} }

// Times are stored as fixed-width UTC text so they sort lexicographically.
const timeLayout = "2006-01-02T15:04:05.000000000Z"

func fmtTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func fmtTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return fmtTime(*t)
}

func parseTime(s string) (time.Time, error) {
	t, err := time.ParseInLocation(timeLayout, s, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time %q: %w", s, err)
	}
	return t, nil
}

func parseTimePtr(s sql.NullString) (*time.Time, error) {
	if !s.Valid {
		return nil, nil
	}
	t, err := parseTime(s.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func toJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal %T: %v", v, err)) // only plain data types are stored
	}
	return string(b)
}

func fromJSON(s string, v any) error {
	if err := json.Unmarshal([]byte(s), v); err != nil {
		return fmt.Errorf("decode stored json: %w", err)
	}
	return nil
}

func notFound(what, id string) error { return fmt.Errorf("%s %q: %w", what, id, domain.ErrNotFound) }

// wrapNoRows maps sql.ErrNoRows to ErrNotFound.
func wrapNoRows(err error, what, id string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return notFound(what, id)
	}
	return err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// pageArgs validates the offset cursor and returns the offset and effective limit.
func pageArgs(cursor string, limit int) (off, lim int, err error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if cursor != "" {
		off, err = strconv.Atoi(cursor)
		if err != nil || off < 0 {
			return 0, 0, fmt.Errorf("bad cursor %q: %w", cursor, domain.ErrValidation)
		}
	}
	return off, limit, nil
}

// finishPage trims the one extra row fetched and sets NextToken.
func finishPage[T any](items []T, off, lim int) domain.Page[T] {
	p := domain.Page[T]{Items: items}
	if len(items) > lim {
		p.Items = items[:lim]
		p.NextToken = strconv.Itoa(off + lim)
	}
	return p
}
