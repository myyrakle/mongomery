package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/myyrakle/mongomery/internal/migrator"
)

type cliMode string

const (
	cliModeVerify cliMode = "verify"
	cliModeSchema cliMode = "schema"
	cliModeCopy   cliMode = "copy"
	cliModeFull   cliMode = ""
)

func main() {
	var configPath string
	var command string
	flag.StringVar(&configPath, "config", "", "Path to JSON config file")
	flag.StringVar(&command, "command", "", "명령 모드: verify | schema | copy")
	flag.Parse()

	mode, err := resolveMode(command, flag.Args())
	if err != nil {
		log.Fatal(err)
	}

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
	defer func() {
		if err := m.Close(context.Background()); err != nil {
			log.Printf("failed to disconnect mongo clients: %v", err)
		}
	}()

	switch mode {
	case cliModeVerify:
		if err := m.VerifyConnections(ctx); err != nil {
			log.Fatal(err)
		}
	case cliModeSchema:
		if err := m.ReplicateSchema(ctx); err != nil {
			log.Fatal(err)
		}
	case cliModeCopy:
		if err := m.CopyData(ctx); err != nil {
			log.Fatal(err)
		}
	case cliModeFull:
		if err := m.Run(ctx); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown mode: %s", mode)
	}
}

func resolveMode(flagCommand string, args []string) (cliMode, error) {
	normalizedFlagCommand := strings.ToLower(strings.TrimSpace(flagCommand))
	var positionalMode string

	if len(args) > 1 {
		return "", fmt.Errorf("too many positional arguments, expected only command")
	}
	if len(args) == 1 {
		positionalMode = strings.ToLower(strings.TrimSpace(args[0]))
	}

	if normalizedFlagCommand != "" && positionalMode != "" && normalizedFlagCommand != positionalMode {
		return "", fmt.Errorf("conflicting command values: -command=%s and positional=%s", normalizedFlagCommand, positionalMode)
	}

	mode := normalizedFlagCommand
	if mode == "" {
		mode = positionalMode
	}
	if mode == "" {
		// no mode defaults to full flow: verify -> schema -> copy
		return cliModeFull, nil
	}
	if err := validateMode(mode); err != nil {
		return "", err
	}
	return cliMode(mode), nil
}

func validateMode(mode string) error {
	switch cliMode(mode) {
	case cliModeVerify, cliModeSchema, cliModeCopy:
		return nil
	default:
		return errors.New("invalid mode: verify | schema | copy")
	}
}
