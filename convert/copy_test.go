package convert

import (
	"path"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/testutil"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
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

func TestVerifyKeysFail(t *testing.T) {
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/singleSpec")

	c := NewCopy(dir, DefaultSpec, SingleSpec, nil, func(string, ...interface{}) {})
	err := c.Run()
	if err != nil {
		t.Fatal(err)
	}

	r, err := repo.OpenDatastore(dir, SingleSpec)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Delete(ds.NewKey("/blocks/NotARandomKey"))
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = c.Verify()
	if err.Error() != "Key /blocks/NotARandomKey was not present in new datastore" {
		t.Fatal(err)
	}
}
