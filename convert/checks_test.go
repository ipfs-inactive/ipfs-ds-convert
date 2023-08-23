package convert_test

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/revert"
	"github.com/ipfs/ipfs-ds-convert/testutil"

	lock "github.com/ipfs/go-fs-lock"
)

func TestInvalidRepoVersion(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.WriteFile(path.Join(dir, "version"), []byte("147258369"), 0664)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected invalid repo version"))
	}

	if !strings.Contains(err.Error(), "unsupported fsrepo version: 147258369") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestLockedRepo(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	unlock, err := lock.Lock(dir, "repo.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock.Close()

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected invalid repo version"))
	}

	if !strings.Contains(err.Error(), "lock is already held") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestNoSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.Remove(path.Join(dir, repo.SpecsFile))
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected no such file or directory"))
	}

	if !strings.Contains(err.Error(), "datastore_spec: ") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestNoVersion(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.Remove(path.Join(dir, "version"))
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected no such file or directory"))
	}

	if !strings.Contains(err.Error(), "version: ") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidVersion(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.WriteFile(path.Join(dir, "version"), []byte("a"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected strconv.Atoi: parsing"))
	}

	if !strings.Contains(err.Error(), `strconv.Atoi: parsing "a": invalid syntax`) {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidSpecJson(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.WriteFile(path.Join(dir, repo.SpecsFile), []byte("}"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected invalid character '}' looking for beginning of value"))
	}

	if !strings.Contains(err.Error(), "invalid character '}' looking for beginning of value") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidSpecFile(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.WriteFile(path.Join(dir, repo.SpecsFile), []byte("{}"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected validating datastore_spec spec: invalid type entry in config"))
	}

	if !strings.Contains(err.Error(), "validating datastore_spec spec: invalid type entry in config") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestNoConfig(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.Remove(path.Join(dir, repo.ConfigFile))
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected no such file or directory"))
	}

	if !strings.Contains(err.Error(), "config: ") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidConfigJson(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.WriteFile(path.Join(dir, repo.ConfigFile), []byte("}"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected invalid character '}' looking for beginning of value"))
	}

	if !strings.Contains(err.Error(), "invalid character '}' looking for beginning of value") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidConfigFile(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := os.WriteFile(path.Join(dir, repo.ConfigFile), []byte("{}"), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected no 'Datastore' or invalid type in"))
	}

	if !strings.Contains(err.Error(), "no 'Datastore' or invalid type") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}

	err = os.Remove(path.Join(dir, revert.ConvertLog))
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(path.Join(dir, repo.ConfigFile), []byte(`{"Datastore":{}}`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected no 'Datastore.Spec' or invalid type"))
	}

	if !strings.Contains(err.Error(), "no 'Datastore.Spec' or invalid type in") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestInvalidSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/invalidSpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "unsupported type entry in config: notAValidDatastoreType") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestAbsolutePathSpec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the test uses unix paths in test vectors")
	}

	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/absPathSpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "only paths inside ipfs repo are supported") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestReusePathSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/reusePathSpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "path 'datastore' is already in use") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}

func TestROSpec(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")
	if err := os.Chmod(path.Join(dir, repo.SpecsFile), 0400); err != nil {
		t.Fatal(err)
	}

	//Convert!
	err := convert.Convert(dir, false)
	if err == nil {
		t.Fatal(fmt.Errorf("No error, expected error validating datastore spec"))
	}

	if !strings.Contains(err.Error(), "datastore_spec is not writable") {
		t.Fatal(fmt.Errorf("unexpected error: %s", err))
	}
}
