package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/myyrakle/mongomery/internal/migrator"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to JSON config file")
	flag.Parse()

	if configPath == "" {
		log.Fatal("config is required")
	}

	cfg, err := migrator.LoadConfig(filepath.Clean(configPath))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	m, err := migrator.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := m.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
