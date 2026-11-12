package server

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/ylnyorulmaz/pulsar/internal/hub"
)

//go:embed web/*
var webFS embed.FS

// Config for pulse server.
type Config struct {
	Addr   string
	Secret string
}

// Run starts the hub and serves the embedded dashboard.
func Run(cfg Config) error {
	if cfg.Secret == "" {
		return fmt.Errorf("--secret is required")
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8741"
	}

	h := hub.New(hub.Config{Secret: cfg.Secret})

	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		return err
	}
	static := http.FileServer(http.FS(sub))

	mux := http.NewServeMux()
	h.Mount(mux, static)

	return h.Serve(cfg.Addr, mux)
}
