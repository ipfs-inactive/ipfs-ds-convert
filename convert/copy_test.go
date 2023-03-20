package convert

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/testutil"

	ds "github.com/ipfs/go-datastore"
)

var (
	ValidSpec = map[string]interface{}{
		"type":      "flatfs",
		"path":      "blocks",
		"sync":      true,
		"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
	}

	InvalidSpec = map[string]interface{}{}

	DefaultSpec = map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "flatfs",
				"path":       "blocks",
				"sync":       true,
				"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
			},
			map[string]interface{}{
				"mountpoint":  "/",
				"type":        "levelds",
				"path":        "levelDatastore",
				"compression": "none",
			},
		},
	}

	SingleSpec = map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint":  "/",
				"type":        "levelds",
				"path":        "levelDatastore",
				"compression": "none",
			},
		},
	}
)

func TestInvalidSpecLeft(t *testing.T) {
	d, err := os.MkdirTemp(os.TempDir(), "ds-convert-test-")
	if err != nil {
		t.Fatalf(err.Error())
	}

	c := NewCopy(d, InvalidSpec, ValidSpec, nil, func(string, ...interface{}) {})
	err = c.Run()
	if err != nil {
		expect := fmt.Sprintf("error validating datastore spec in %s: invalid type entry in config", filepath.Join(d, "datastore_spec"))
		if strings.Contains(err.Error(), expect) {
			return
		}
		t.Errorf("unexpected error: '%s', expected to get '%s'", err, expect)
	}

	t.Errorf("expected error")
}

func TestInvalidSpecRight(t *testing.T) {
	d, err := os.MkdirTemp(os.TempDir(), "ds-convert-test-")
	if err != nil {
		t.Fatalf(err.Error())
	}

	c := NewCopy(d, ValidSpec, InvalidSpec, nil, func(string, ...interface{}) {})
	err = c.Run()
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("error validating datastore spec in %s: invalid type entry in config", filepath.Join(d, "config"))) {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestOpenNonexist(t *testing.T) {
	d, err := os.MkdirTemp(os.TempDir(), "ds-convert-test-")
	if err != nil {
		t.Fatalf(err.Error())
	}

	p := filepath.Join(d, "hopefully/nonexistent/repo")
	expect := fmt.Sprintf("error opening datastore at %s: mkdir %s: ", p, filepath.Join(p, "blocks"))

	c := NewCopy(p, ValidSpec, ValidSpec, nil, func(string, ...interface{}) {})
	err = c.Run()
	if err != nil {
		if strings.Contains(err.Error(), expect) {
			return
		}
		t.Errorf("unexpected error: %s", err)
		t.Errorf("expected        : %s", expect)
	}

	t.Errorf("expected error")
}

func TestVerifyKeysFail(t *testing.T) {
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, filepath.Join(dir, "config"), "../testfiles/singleSpec")

	c := NewCopy(dir, DefaultSpec, SingleSpec, nil, func(string, ...interface{}) {})
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}

	r, err := repo.OpenDatastore(dir, SingleSpec)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Delete(ds.NewKey("/blocks/NOTARANDOMKEY")); err != nil {
		t.Fatal(err)
	}

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if err := c.Verify(); err.Error() != "key /blocks/NOTARANDOMKEY was not present in new datastore" {
		t.Fatal(err)
	}
}
