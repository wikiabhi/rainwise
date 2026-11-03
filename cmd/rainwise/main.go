// Command rainwise sorts a bookmark library (§13).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wikiabhi/rainwise/internal/app"
	"github.com/wikiabhi/rainwise/internal/config"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	usage = "usage: rainwise serve --config PATH\n       rainwise config validate --config PATH\n"

	// shutdownGrace is how long a stop signal gives in-flight items to finish before
	// they're cancelled. Their job resumes from its checkpoint either way.
	shutdownGrace = 30 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	// After the first signal the default action returns, so a second one kills.
	context.AfterFunc(ctx, stop)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

// run is main without the process around it. signals ends when serve should stop.
func run(signals context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	switch {
	case len(args) >= 1 && args[0] == "serve":
		path, ok := configFlag(args[1:], stderr)
		if !ok {
			return exitUsage
		}
		return serve(signals, path, stdout, stderr, getenv)
	case len(args) >= 2 && args[0] == "config" && args[1] == "validate":
		path, ok := configFlag(args[2:], stderr)
		if !ok {
			return exitUsage
		}
		return validate(path, stdout, stderr, getenv)
	}
	fmt.Fprint(stderr, usage)
	return exitUsage
}

func configFlag(args []string, stderr io.Writer) (string, bool) {
	fs := flag.NewFlagSet("rainwise", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	path := fs.String("config", "", "path to the config file")
	if err := fs.Parse(args); err != nil {
		return "", false
	}
	if *path == "" || fs.NArg() > 0 {
		fmt.Fprint(stderr, usage)
		return "", false
	}
	return *path, true
}

func validate(path string, stdout, stderr io.Writer, getenv func(string) string) int {
	cfg, err := config.Load(path, getenv)
	if err == nil {
		err = app.Check(cfg, BuildDeps{})
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	fmt.Fprintln(stdout, "config ok")
	return exitOK
}

func serve(signals context.Context, path string, stdout, stderr io.Writer, getenv func(string) string) int {
	cfg, err := config.Load(path, getenv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	// Core code logs through the default logger in places, so this comes first.
	slog.SetDefault(slog.New(slog.NewJSONHandler(stdout, nil)))
	a, err := Build(cfg, BuildDeps{})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}

	// Jobs outlive the signal: Shutdown lets their in-flight items finish, which
	// cancelling this context wouldn't.
	a.Start(context.Background())
	served := make(chan error, 1)
	go func() { served <- a.Serve() }()

	code := exitOK
	select {
	case <-signals.Done():
		slog.Info("stopping")
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(stderr, err)
			code = exitError
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		fmt.Fprintln(stderr, err)
		code = exitError
	}
	return code
}
