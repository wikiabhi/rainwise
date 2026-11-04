package httpapi

import (
	"net/http"

	"github.com/wikiabhi/rainwise/internal/domain"
)

// snapshotView keeps lists out of null. Its IDs are structured, as in an export.
func snapshotView(s domain.Snapshot) domain.Snapshot {
	if s.Collections == nil {
		s.Collections = []domain.Collection{}
	}
	if s.CreatedCollections == nil {
		s.CreatedCollections = []domain.ID{}
	}
	return s
}

func (s *server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	cursor, limit, err := listParams(r)
	if err != nil {
		s.fail(w, err)
		return
	}
	p, err := s.Store.Snapshots().ListSnapshots(r.Context(), cursor, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	for i := range p.Items {
		p.Items[i] = snapshotView(p.Items[i])
	}
	writeJSON(w, http.StatusOK, newPage(p.Items, p.NextToken))
}

func (s *server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.Store.Snapshots().GetSnapshot(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshotView(snap))
}

func (s *server) exportSnapshot(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Look first, so a missing snapshot is still a 404 before any streaming.
	if _, err := s.Store.Snapshots().GetSnapshot(r.Context(), id); err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="`+id+`.json"`)
	if err := s.Snapshots.Export(r.Context(), id, w); err != nil {
		// The status is sent by now; the client sees truncated JSON.
		s.log.Error("snapshot export", "snapshot", id, "error", err)
	}
}
