package convert

import (
	"strings"
	"testing"
)

var (
	ValidSpec = map[string]interface{}{
		"type":      "flatfs",
		"path":      "blocks",
		"sync":      true,
		"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
	}

	InvalidSpec = map[string]interface{}{}
)

func TestInvalidSpecLeft(t *testing.T) {
	c := NewCopy("/tmp/", InvalidSpec, ValidSpec, nil, func(string, ...interface{}) {})
	err := c.Run()
	if err != nil {
		if strings.Contains(err.Error(), "error validating datastore spec in /tmp/datastore_spec: invalid type entry in config") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidSpecRight(t *testing.T) {
	c := NewCopy("/tmp/", ValidSpec, InvalidSpec, nil, func(string, ...interface{}) {})
	err := c.Run()
	if err != nil {
		if strings.Contains(err.Error(), "error validating datastore spec in /tmp/config: invalid type entry in config") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestOpenNonexist(t *testing.T) {
	c := NewCopy("/tmp/hopefully/nonexistent/repo", ValidSpec, ValidSpec, nil, func(string, ...interface{}) {})
	err := c.Run()
	if err != nil {
		if strings.Contains(err.Error(), "error opening datastore at /tmp/hopefully/nonexistent/repo: mkdir /tmp/hopefully/nonexistent/repo/blocks: no such file or directory") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}
