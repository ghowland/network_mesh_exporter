// Package api exposes the metrics endpoint and the read and control
// endpoints. Every read comes from memory, so a request inside the
// persistence debounce window still returns current data.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/example/mesh/internal/config"
	"github.com/example/mesh/internal/inventory"
	"github.com/example/mesh/internal/metrics"
	"github.com/example/mesh/internal/provider"
	"github.com/example/mesh/internal/reconcile"
	"github.com/example/mesh/internal/runner"
	"github.com/example/mesh/internal/state"
	"github.com/example/mesh/internal/health"
	"github.com/example/mesh/internal/zone"
)

// Deps holds the live components the handlers read.
type Deps struct {
	Config    func() *config.Config
	Store     *inventory.Store
	Rule      func() *zone.Rule
	State     *state.Store
	Health    *health.Tracker
	Runner    *runner.Runner
	Providers *provider.Manager
	Loop      *reconcile.Loop
	Metrics   *metrics.Registry
	Version   string
	StartedAt time.Time
}

// Server serves the HTTP surface.
type Server struct {
	cfg  config.APIConfig
	deps Deps
	srv  *http.Server
}

// New creates a Server.
func New(cfg config.APIConfig, d Deps) *Server {
	s := &Server{cfg: cfg, deps: d}
	s.srv = &http.Server{
		Addr:              cfg.Listen,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// Run serves until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := s.srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(sctx)
		return nil
	}
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", s.deps.Metrics.Handler())
	mux.HandleFunc("/state", s.handleState)
	mux.HandleFunc("/inventory", s.handleInventory)
	mux.HandleFunc("/zones", s.handleZones)
	mux.HandleFunc("/pairings", s.handlePairings)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/tasks", s.handleTasks)
	mux.HandleFunc("/config", s.handleConfig)
	mux.HandleFunc("/reconcile", s.handleReconcile)
	mux.HandleFunc("/refresh", s.handleRefresh)
	mux.HandleFunc("/livez", s.handleLivez)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/", s.handleIndex)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version":    s.deps.Version,
		"node_id":    s.deps.Config().NodeID,
		"started_at": s.deps.StartedAt,
		"uptime":     time.Since(s.deps.StartedAt).Truncate(time.Second).String(),
		"endpoints": []string{
			"/metrics", "/state", "/inventory", "/zones", "/pairings",
			"/health", "/tasks", "/config", "/reconcile", "/refresh",
			"/livez", "/readyz",
		},
	})
}

// handleState reads the in-memory state, not the file, so a request
// inside the persistence debounce window still returns current data.
func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	st := s.deps.State.Current()
	stats, delta, at := s.deps.Loop.Last()
	writeJSON(w, http.StatusOK, map[string]any{
		"state":          st,
		"persist":        s.deps.State.Stats(),
		"last_reconcile": at,
		"last_stats":     stats,
		"last_delta":     delta,
	})
}

func (s *Server) handleInventory(w http.ResponseWriter, r *http.Request) {
	snap := s.deps.Store.Snapshot()
	rule := s.deps.Rule()

	type entry struct {
		inventory.HostRecord
		Zone string `json:"zone"`
	}
	out := make([]entry, 0, snap.Len())
	for _, h := range snap.Hosts {
		e := entry{HostRecord: h}
		if k, ok := rule.Apply(h); ok {
			e.Zone = string(k)
		}
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generation": snap.Generation,
		"taken_at":   snap.TakenAt,
		"collisions": snap.Collisions,
		"sources":    s.deps.Providers.Status(),
		"hosts":      out,
	})
}

func (s *Server) handleZones(w http.ResponseWriter, r *http.Request) {
	idx := zone.Build(s.deps.Rule(), s.deps.Store.Snapshot())
	type z struct {
		Zone    string   `json:"zone"`
		Members []string `json:"members"`
		Count   int      `json:"count"`
	}
	out := make([]z, 0, len(idx.Zones))
	for _, k := range idx.Zones {
		out = append(out, z{Zone: string(k), Members: idx.Members[k], Count: len(idx.Members[k])})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"rule":       s.deps.Rule().String(),
		"zones":      out,
		"unresolved": idx.Unresolved,
	})
}

func (s *Server) handlePairings(w http.ResponseWriter, r *http.Request) {
	st := s.deps.State.Current()
	type p struct {
		Key      string    `json:"key"`
		ZoneA    string    `json:"zone_a"`
		ZoneB    string    `json:"zone_b"`
		Slots    int       `json:"slots"`
		Filled   int       `json:"filled"`
		RemoveAt time.Time `json:"remove_at,omitempty"`
	}
	out := make([]p, 0, len(st.Pairings))
	for key, pr := range st.Pairings {
		filled := 0
		for _, sl := range pr.Slots {
			if sl.Filled() {
				filled++
			}
		}
		out = append(out, p{
			Key: key, ZoneA: pr.ZoneA, ZoneB: pr.ZoneB,
			Slots: len(pr.Slots), Filled: filled, RemoveAt: pr.RemoveAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pairings":    out,
		"super_hosts": st.SuperHosts,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"hosts":  s.deps.Health.All(),
		"counts": countsByName(s.deps.Health),
	})
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id": s.deps.Config().NodeID,
		"tasks":   s.deps.Runner.Tasks(),
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.deps.Config().Redacted())
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
		return
	}
	delta, stats, err := s.deps.Loop.RunNow(reconcile.TriggerAPI)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": err.Error(), "stats": stats,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"delta": delta, "stats": stats})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use POST"})
		return
	}
	source := r.URL.Query().Get("source")
	if err := s.deps.Providers.Refresh(source); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "refresh requested"})
}

func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports ready once a provider has produced hosts and one
// reconcile has completed.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	snap := s.deps.Store.Snapshot()
	ready := snap.Len() > 0 && s.deps.Loop.Ready()
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"ready":     ready,
		"hosts":     snap.Len(),
		"reconcile": s.deps.Loop.Ready(),
	})
}

func countsByName(t *health.Tracker) map[string]int {
	out := make(map[string]int)
	for st, n := range t.Counts() {
		out[st.String()] = n
	}
	return out
}

// writeJSON encodes a value with the correct content type.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

