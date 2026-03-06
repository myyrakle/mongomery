package main

import "testing"

func TestResolveMode_Default(t *testing.T) {
	mode, err := resolveMode("", []string{})
	if err != nil {
		t.Fatalf("resolveMode returned error: %v", err)
	}
	if mode != cliModeRun {
		t.Fatalf("expected default mode %s, got %s", cliModeRun, mode)
	}
}

func TestResolveMode_Positional(t *testing.T) {
	mode, err := resolveMode("", []string{"schema"})
	if err != nil {
		t.Fatalf("resolveMode returned error: %v", err)
	}
	if mode != cliModeSchema {
		t.Fatalf("expected mode %s, got %s", cliModeSchema, mode)
	}
}

func TestResolveMode_Flag(t *testing.T) {
	mode, err := resolveMode("copy", []string{})
	if err != nil {
		t.Fatalf("resolveMode returned error: %v", err)
	}
	if mode != cliModeCopy {
		t.Fatalf("expected mode %s, got %s", cliModeCopy, mode)
	}
}

func TestResolveMode_Conflict(t *testing.T) {
	_, err := resolveMode("verify", []string{"schema"})
	if err == nil {
		t.Fatal("expected mode conflict error")
	}
}

func TestResolveMode_TooManyArgs(t *testing.T) {
	_, err := resolveMode("", []string{"schema", "copy"})
	if err == nil {
		t.Fatal("expected too many args error")
	}
}

func TestValidateMode_Invalid(t *testing.T) {
	if err := validateMode("invalid"); err == nil {
		t.Fatal("expected invalid mode error")
	}
}
