package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/wikiabhi/rainwise/internal/domain"
)

type jobStore struct{ db *sql.DB }

const jobCols = `id, type, status, provider, taxonomy_id, dry_run, params, scope, from_dry_run,
	snapshot_id, idempotency_key, error, progress, checkpoint, created_at, started_at, finished_at`

type scanner interface{ Scan(dest ...any) error }

func scanJob(r scanner) (domain.Job, error) {
	var (
		j                         domain.Job
		typ, status               string
		dry                       int
		params, scope, prog, ckpt string
		idem                      sql.NullString
		created                   string
		started, finished         sql.NullString
	)
	if err := r.Scan(&j.ID, &typ, &status, &j.Provider, &j.TaxonomyID, &dry, &params, &scope, &j.FromDryRun,
		&j.SnapshotID, &idem, &j.Error, &prog, &ckpt, &created, &started, &finished); err != nil {
		return domain.Job{}, err
	}
	j.Type, j.Status, j.DryRun, j.IdempotencyKey = domain.JobType(typ), domain.JobStatus(status), dry != 0, idem.String
	for _, p := range []struct {
		s string
		v any
	}{{params, &j.Params}, {scope, &j.Scope}, {prog, &j.Progress}, {ckpt, &j.Checkpoint}} {
		if err := fromJSON(p.s, p.v); err != nil {
			return domain.Job{}, err
		}
	}
	var err error
	if j.CreatedAt, err = parseTime(created); err != nil {
		return domain.Job{}, err
	}
	if j.StartedAt, err = parseTimePtr(started); err != nil {
		return domain.Job{}, err
	}
	if j.FinishedAt, err = parseTimePtr(finished); err != nil {
		return domain.Job{}, err
	}
	return j, nil
}

func jobArgs(j domain.Job) []any {
	var idem any
	if j.IdempotencyKey != "" {
		idem = j.IdempotencyKey
	}
	return []any{j.ID, string(j.Type), string(j.Status), j.Provider, j.TaxonomyID, boolInt(j.DryRun),
		toJSON(j.Params), toJSON(j.Scope), j.FromDryRun, j.SnapshotID, idem, j.Error,
		toJSON(j.Progress), toJSON(j.Checkpoint), fmtTime(j.CreatedAt), fmtTimePtr(j.StartedAt), fmtTimePtr(j.FinishedAt)}
}

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func (s jobStore) CreateJob(ctx context.Context, j domain.Job) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO jobs (`+jobCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, jobArgs(j)...)
	if isUnique(err) {
		return fmt.Errorf("job %q or its idempotency key exists: %w", j.ID, domain.ErrConflict)
	}
	return err
}

func (s jobStore) GetJob(ctx context.Context, id string) (domain.Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
	if err != nil {
		return domain.Job{}, wrapNoRows(err, "job", id)
	}
	return j, nil
}

func (s jobStore) findOne(ctx context.Context, where string, args ...any) (domain.Job, bool, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE `+where+` LIMIT 1`, args...))
	if err == sql.ErrNoRows {
		return domain.Job{}, false, nil
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	return j, true, nil
}

func (s jobStore) FindJobByIdempotencyKey(ctx context.Context, key string) (domain.Job, bool, error) {
	if key == "" {
		return domain.Job{}, false, nil
	}
	return s.findOne(ctx, `idempotency_key = ?`, key)
}

func (s jobStore) FindActiveJob(ctx context.Context, provider string, t domain.JobType) (domain.Job, bool, error) {
	return s.findOne(ctx, `provider = ? AND type = ? AND status IN ('queued','running') ORDER BY created_at, rowid`, provider, string(t))
}

func (s jobStore) ListJobs(ctx context.Context, cursor string, limit int) (domain.Page[domain.Job], error) {
	off, lim, err := pageArgs(cursor, limit)
	if err != nil {
		return domain.Page[domain.Job]{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?`, lim+1, off)
	if err != nil {
		return domain.Page[domain.Job]{}, err
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return domain.Page[domain.Job]{}, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return domain.Page[domain.Job]{}, err
	}
	return finishPage(out, off, lim), nil
}

func (s jobStore) UpdateJob(ctx context.Context, j domain.Job) error {
	a := jobArgs(j)
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET type=?, status=?, provider=?, taxonomy_id=?, dry_run=?, params=?, scope=?,
		from_dry_run=?, snapshot_id=?, idempotency_key=?, error=?, progress=?, checkpoint=?, created_at=?, started_at=?, finished_at=?
		WHERE id = ?`, append(a[1:], j.ID)...)
	if isUnique(err) {
		return fmt.Errorf("idempotency key %q exists: %w", j.IdempotencyKey, domain.ErrConflict)
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return notFound("job", j.ID)
	}
	return nil
}

func (s jobStore) ClaimNextQueued(ctx context.Context, provider string, dryRun bool) (domain.Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, false, err
	}
	defer tx.Rollback()
	j, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs
		WHERE status = 'queued' AND provider = ? AND dry_run = ? ORDER BY created_at, rowid LIMIT 1`, provider, boolInt(dryRun)))
	if err == sql.ErrNoRows {
		return domain.Job{}, false, nil
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status = 'running' WHERE id = ?`, j.ID); err != nil {
		return domain.Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Job{}, false, err
	}
	j.Status = domain.JobRunning
	return j, true, nil
}

func (s jobStore) ListRunning(ctx context.Context) ([]domain.Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE status = 'running' ORDER BY created_at, rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s jobStore) SaveDecision(ctx context.Context, d domain.DecisionRecord) error {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE id = ?`, d.JobID).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return notFound("job", d.JobID)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO job_decisions (job_id, bookmark, body) VALUES (?, ?, ?)
		ON CONFLICT(job_id, bookmark) DO UPDATE SET body = excluded.body`, d.JobID, d.Bookmark.String(), toJSON(d))
	return err
}

func (s jobStore) GetDecision(ctx context.Context, jobID string, b domain.ID) (domain.DecisionRecord, bool, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `SELECT body FROM job_decisions WHERE job_id = ? AND bookmark = ?`, jobID, b.String()).Scan(&body)
	if err == sql.ErrNoRows {
		return domain.DecisionRecord{}, false, nil
	}
	if err != nil {
		return domain.DecisionRecord{}, false, err
	}
	var d domain.DecisionRecord
	if err := fromJSON(body, &d); err != nil {
		return domain.DecisionRecord{}, false, err
	}
	return d, true, nil
}

func (s jobStore) ListDecisions(ctx context.Context, jobID, cursor string, limit int) (domain.Page[domain.DecisionRecord], error) {
	off, lim, err := pageArgs(cursor, limit)
	if err != nil {
		return domain.Page[domain.DecisionRecord]{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT body FROM job_decisions WHERE job_id = ? ORDER BY seq LIMIT ? OFFSET ?`, jobID, lim+1, off)
	if err != nil {
		return domain.Page[domain.DecisionRecord]{}, err
	}
	defer rows.Close()
	var out []domain.DecisionRecord
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return domain.Page[domain.DecisionRecord]{}, err
		}
		var d domain.DecisionRecord
		if err := fromJSON(body, &d); err != nil {
			return domain.Page[domain.DecisionRecord]{}, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return domain.Page[domain.DecisionRecord]{}, err
	}
	return finishPage(out, off, lim), nil
}
