package httpapi

import (
	"net/http"
	"time"

	"github.com/wikiabhi/rainwise/internal/core/jobs"
	"github.com/wikiabhi/rainwise/internal/domain"
)

type createJobRequest struct {
	Type       domain.JobType   `json:"type"`
	Provider   string           `json:"provider"`
	Taxonomy   string           `json:"taxonomy"`
	DryRun     bool             `json:"dry_run"`
	Params     domain.JobParams `json:"params"`
	Scope      domain.Scope     `json:"scope"`
	FromDryRun string           `json:"from_dry_run"`
}

type scopeView struct {
	Collections []string `json:"collections"`
	Tags        []string `json:"tags"`
	IDs         []string `json:"ids"`
}

type jobView struct {
	ID         string             `json:"id"`
	Type       domain.JobType     `json:"type"`
	Status     domain.JobStatus   `json:"status"`
	Provider   string             `json:"provider"`
	Taxonomy   string             `json:"taxonomy,omitempty"`
	DryRun     bool               `json:"dry_run"`
	Params     domain.JobParams   `json:"params"`
	Scope      scopeView          `json:"scope"`
	FromDryRun *string            `json:"from_dry_run"`
	Progress   domain.JobProgress `json:"progress"`
	SnapshotID *string            `json:"snapshot_id"`
	CreatedAt  time.Time          `json:"created_at"`
	StartedAt  *time.Time         `json:"started_at"`
	FinishedAt *time.Time         `json:"finished_at"`
	Error      *string            `json:"error"`
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func viewJob(j domain.Job) jobView {
	return jobView{
		ID: j.ID, Type: j.Type, Status: j.Status, Provider: j.Provider, Taxonomy: j.TaxonomyID,
		DryRun: j.DryRun, Params: j.Params,
		Scope:      scopeView{nonNil(j.Scope.Collections), nonNil(j.Scope.Tags), nonNil(j.Scope.IDs)},
		FromDryRun: strPtr(j.FromDryRun), Progress: j.Progress, SnapshotID: strPtr(j.SnapshotID),
		CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, Error: strPtr(j.Error),
	}
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	var body createJobRequest
	if err := decodeBody(w, r, &body); err != nil {
		s.fail(w, err)
		return
	}
	if body.Provider == "" {
		body.Provider = s.DefaultProvider
	}
	// Snapshots and restores take no taxonomy (§11).
	if body.Taxonomy == "" && body.Type != domain.JobSnapshot && body.Type != domain.JobRestore {
		body.Taxonomy = s.DefaultTaxonomy
	}
	job, created, err := s.Jobs.Create(r.Context(), jobs.CreateRequest{
		Type: body.Type, Provider: body.Provider, Taxonomy: body.Taxonomy, DryRun: body.DryRun,
		Params: body.Params, Scope: body.Scope, FromDryRun: body.FromDryRun,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	if !created {
		writeJSON(w, http.StatusOK, viewJob(job))
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	writeJSON(w, http.StatusAccepted, viewJob(job))
}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("active") {
	case "":
	case "true":
		s.listActiveJobs(w, r)
		return
	default:
		s.fail(w, badRequest("active must be true"))
		return
	}
	cursor, limit, err := listParams(r)
	if err != nil {
		s.fail(w, err)
		return
	}
	p, err := s.Jobs.List(r.Context(), cursor, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	items := make([]jobView, len(p.Items))
	for i, j := range p.Items {
		items[i] = viewJob(j)
	}
	writeJSON(w, http.StatusOK, newPage(items, p.NextToken))
}

// listActiveJobs answers GET /v1/jobs?active=true with the running jobs, oldest
// first, in one page with no cursor. Queued jobs are claimed promptly while the
// process is awake, so a caller asking whether the service is busy needs only these.
func (s *server) listActiveJobs(w http.ResponseWriter, r *http.Request) {
	running, err := s.Store.Jobs().ListRunning(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	items := make([]jobView, len(running))
	for i, j := range running {
		items[i] = viewJob(j)
	}
	writeJSON(w, http.StatusOK, newPage(items, ""))
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.Jobs.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewJob(j))
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.Jobs.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, viewJob(j))
}

// decisionView is a decision record for any job but a restore. Bookmark and
// collection IDs are "provider:local" strings; a dry run's target may be a
// "planned:<key>" placeholder, shown as stored.
type decisionView struct {
	Bookmark           string          `json:"bookmark"`
	InputHash          string          `json:"input_hash"`
	ObservedCollection string          `json:"observed_collection"`
	ObservedTags       []string        `json:"observed_tags"`
	TargetCollection   *string         `json:"target_collection"`
	AddTags            []string        `json:"add_tags"`
	NewTags            []string        `json:"new_tags"`
	Decision           domain.Decision `json:"decision"`
	Reason             string          `json:"reason"`
	Result             string          `json:"result"`
	AppliedAt          *time.Time      `json:"applied_at"`
}

// restoreDiffView is a restore's decision record as the change it makes.
// ToCollection is null when the restore skipped the bookmark.
type restoreDiffView struct {
	Bookmark       string     `json:"bookmark"`
	FromCollection string     `json:"from_collection"`
	ToCollection   *string    `json:"to_collection"`
	FromTags       []string   `json:"from_tags"`
	ToTags         []string   `json:"to_tags"`
	Reason         string     `json:"reason"`
	Result         string     `json:"result"`
	AppliedAt      *time.Time `json:"applied_at"`
}

func idString(id domain.ID) string {
	if id.IsZero() {
		return ""
	}
	return id.String()
}

func idPtr(id *domain.ID) *string {
	if id == nil {
		return nil
	}
	s := idString(*id)
	return &s
}

func viewDecision(d domain.DecisionRecord) decisionView {
	dec := d.Decision
	if dec.Tags == nil {
		dec.Tags = []domain.TagSuggestion{}
	}
	return decisionView{
		Bookmark: idString(d.Bookmark), InputHash: d.InputHash,
		ObservedCollection: idString(d.ObservedCollection), ObservedTags: nonNil(d.ObservedTags),
		TargetCollection: idPtr(d.TargetCollection), AddTags: nonNil(d.AddTags), NewTags: nonNil(d.NewTags),
		Decision: dec, Reason: d.Reason, Result: d.Result, AppliedAt: d.AppliedAt,
	}
}

func viewRestoreDiff(d domain.DecisionRecord) restoreDiffView {
	return restoreDiffView{
		Bookmark: idString(d.Bookmark), FromCollection: idString(d.ObservedCollection),
		ToCollection: idPtr(d.TargetCollection), FromTags: nonNil(d.ObservedTags), ToTags: nonNil(d.SetTags),
		Reason: d.Reason, Result: d.Result, AppliedAt: d.AppliedAt,
	}
}

func (s *server) jobDecisions(w http.ResponseWriter, r *http.Request) {
	cursor, limit, err := listParams(r)
	if err != nil {
		s.fail(w, err)
		return
	}
	job, err := s.Jobs.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	p, err := s.Store.Jobs().ListDecisions(r.Context(), job.ID, cursor, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	if job.Type == domain.JobRestore {
		items := make([]restoreDiffView, len(p.Items))
		for i, d := range p.Items {
			items[i] = viewRestoreDiff(d)
		}
		writeJSON(w, http.StatusOK, newPage(items, p.NextToken))
		return
	}
	items := make([]decisionView, len(p.Items))
	for i, d := range p.Items {
		items[i] = viewDecision(d)
	}
	writeJSON(w, http.StatusOK, newPage(items, p.NextToken))
}

func (s *server) proposedTags(w http.ResponseWriter, r *http.Request) {
	tags, err := jobs.ProposedTags(r.Context(), s.Store, r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newPage(tags, ""))
}
