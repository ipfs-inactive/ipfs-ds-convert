package revert_test

import (
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/revert"
	"github.com/ipfs/ipfs-ds-convert/testutil"

	lock "gx/ipfs/QmWi28zbQG6B1xfaaWx5cYoLn3kBFU6pQ6GWQNRV5P6dNe/lock"
)

func TestBasicConvertRevert(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := testutil.PrepareTest(t, 1000, 1000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if err != nil {
		t.Fatal(err)
	}

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/defaultSpec")

	testutil.FinishTest(t, dir, s1, s2, 1000, 1000)
}

func TestBasicConvertCleanup(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, true)
	if err != nil {
		t.Fatal(err)
	}

	testutil.FinishTest(t, dir, s1, s2, 100, 100)
}

func TestBasicConvertRevertFix(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, true, false)
	if err != nil {
		t.Fatal(err)
	}

	testutil.FinishTest(t, dir, s1, s2, 100, 100)
}

func TestConvertRevertLocked(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	unlock, err := lock.Lock(filepath.Join(dir, "repo.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock.Close()

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "already locked") {
		t.Fatal(err)
	}
}

func TestConvertNoKeepRevert(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "/convertlog: no such file or directory") {
		t.Fatal(err)
	}
}

func TestBasicConvertRevertUnknownStep(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"unknownactiontype","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "unknown revert step 'unknownactiontype'") {
		t.Fatal(err)
	}
}

func TestBasicConvertRevertNoForce(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, false, false, false)
	if !strings.Contains(err.Error(), "last conversion was successful, run with --force to revert") {
		t.Fatal(err)
	}
}

func TestBasicConvertRevertInvalidArgs(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionRemove+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert remove: arg count 0 != 1") {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionMove+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert move: arg count 0 != 2") {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionMkdir+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert mkdir: arg count 0 != 1") {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionCleanup+`","arg":["a"]}`), 0600)

	err = revert.Revert(dir, true, false, true)
	if !strings.Contains(err.Error(), "cannot cleanup after failed conversion") {
		t.Fatal(err)
	}

	ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionCleanup+`","arg":[]}`+"\n"+`{"action":"`+revert.ActionDone+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, true)
	if !strings.Contains(err.Error(), "cleanup arg count 0 != 1") {
		t.Fatal(err)
	}
}

func TestRevertMkdirChecks(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"mkdir","arg":["`+path.Join(dir, revert.ConvertLog)+`"]}`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert mkdir: destination ") || !strings.Contains(err.Error(), " did exist") {
		t.Fatal(err)
	}
}

func TestRevertMoveChecks(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	err := ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"mv","arg":["nonexistentfile","config"]}`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if err.Error() != "revert move: source file 'nonexistentfile' didn't exist" {
		t.Fatal(err)
	}

	err = ioutil.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"mv","arg":["`+path.Join(dir, repo.ConfigFile)+`","`+path.Join(dir, repo.ConfigFile)+`"]}`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert move: destination ") || !strings.Contains(err.Error(), " did exist") {
		t.Fatal(err)
	}
}
