package convert_test

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"github.com/ipfs/ipfs-ds-convert/testutil"

	lock "gx/ipfs/QmWi28zbQG6B1xfaaWx5cYoLn3kBFU6pQ6GWQNRV5P6dNe/lock"
)

func TestInvalidRepoVersion(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := prepareTest(t, 10, 10)
	defer _close(t)

	err := ioutil.WriteFile(path.Join(dir, "version"), []byte("147258369"), 0664)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected invalid repo version"))
	}

	if !strings.Contains(err.Error(), "unsupported fsrepo version: 147258369") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestLockedRepo(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := prepareTest(t, 10, 10)
	defer _close(t)

	unlock, err := lock.Lock(filepath.Join(dir, "repo.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock.Close()

	//Convert!
	err = convert.Convert(dir)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected invalid repo version"))
	}

	if !strings.Contains(err.Error(), "already locked") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := prepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/invalidSpec")

	//Convert!
	err := convert.Convert(dir)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "unsupported type entry in config: notAValidDatastoreType") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestAbsolutePathSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := prepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/absPathSpec")

	//Convert!
	err := convert.Convert(dir)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "only paths inside ipfs repo are supported") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestReusePathSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := prepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/reusePathSpec")

	//Convert!
	err := convert.Convert(dir)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "path 'datastore' is already in use") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}
