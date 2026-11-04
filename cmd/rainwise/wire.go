package main

import (
	"github.com/wikiabhi/rainwise/internal/app"
	"github.com/wikiabhi/rainwise/internal/config"
)

// The wiring itself is in internal/app, since a main package can't be imported and
// the end-to-end tests have to build the same thing.
type (
	App       = app.App
	BuildDeps = app.BuildDeps
)

// Build wires the service from cfg. deps holds optional test overrides.
func Build(cfg *config.Config, deps BuildDeps) (*App, error) { return app.Build(cfg, deps) }
