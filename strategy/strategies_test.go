package strategy

import (
	"strings"
	"testing"
)

var (
	EmptySpec = map[string]interface{}{}

	NoMountSpec = map[string]interface{}{
		"type": "mount",
	}

	InvalidMountSpec = map[string]interface{}{
		"type":   "mount",
		"mounts": "/",
	}

	EmptyMountSpec = map[string]interface{}{
		"type":   "mount",
		"mounts": []interface{}{},
	}
)

func TestValidateCopyEmptySpec(t *testing.T) {
	err := validateCopySpec(EmptySpec)
	if err != nil {
		if strings.Contains(err.Error(), "copy spec has no type or field type is invalid") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestNoMountEmptySpec(t *testing.T) {
	err := validateCopySpec(NoMountSpec)
	if err != nil {
		if strings.Contains(err.Error(), "copy spec has no mounts field") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidMountsEmptySpec(t *testing.T) {
	err := validateCopySpec(InvalidMountSpec)
	if err != nil {
		if strings.Contains(err.Error(), "copy spec has invalid mounts field type") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestEmptyMountsEmptySpec(t *testing.T) {
	err := validateCopySpec(EmptyMountSpec)
	if err != nil {
		if strings.Contains(err.Error(), "copy spec has empty mounts field") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}
