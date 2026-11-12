package hub

import (
	"log"
	"net/http"
)

// Mount registers hub WebSocket routes on mux. If static is non-nil it is served at /.
func (s *Server) Mount(mux *http.ServeMux, static http.Handler) {
	if static != nil {
		mux.Handle("/", static)
	}
	mux.HandleFunc("/ws/runner", s.handleRunnerWS)
	mux.HandleFunc("/ws/dashboard", s.handleDashboardWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

// Serve starts the HTTP server with the given handler tree.
func (s *Server) Serve(addr string, mux *http.ServeMux) error {
	log.Printf("pulsar hub listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
