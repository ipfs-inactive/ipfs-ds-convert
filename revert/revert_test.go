package revert_test

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/revert"
	"github.com/ipfs/ipfs-ds-convert/testutil"

	lock "github.com/ipfs/go-fs-lock"
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

	unlock, err := lock.Lock(dir, "repo.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock.Close()

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "lock is already held") {
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
	if !strings.Contains(err.Error(), "/convertlog: ") {
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

	os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"unknownactiontype","arg":[]}`), 0600)

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

	os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionRemove+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert remove: arg count 0 != 1") {
		t.Fatal(err)
	}

	os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionMove+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert move: arg count 0 != 2") {
		t.Fatal(err)
	}

	os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionMkdir+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert mkdir: arg count 0 != 1") {
		t.Fatal(err)
	}

	os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionCleanup+`","arg":["a"]}`), 0600)

	err = revert.Revert(dir, true, false, true)
	if !strings.Contains(err.Error(), "cannot cleanup after failed conversion") {
		t.Fatal(err)
	}

	os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(`{"action":"`+revert.ActionCleanup+`","arg":[]}`+"\n"+`{"action":"`+revert.ActionDone+`","arg":[]}`), 0600)

	err = revert.Revert(dir, true, false, true)
	if !strings.Contains(err.Error(), "cleanup arg count 0 != 1") {
		t.Fatal(err)
	}
}

func TestRevertMkdirChecks(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 10, 10)
	defer _close(t)

	l, err := revert.ActionMkdir.Line(path.Join(dir, revert.ConvertLog))
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(l), 0600)
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

	l, err := revert.ActionMove.Line("nonexistentfile", "config")
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(l), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if err.Error() != "revert move: source file 'nonexistentfile' didn't exist" {
		t.Fatal(err)
	}

	l, err = revert.ActionMove.Line(path.Join(dir, repo.ConfigFile), path.Join(dir, repo.ConfigFile))
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(path.Join(dir, revert.ConvertLog), []byte(l), 0600)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, false, false)
	if !strings.Contains(err.Error(), "revert move: destination ") || !strings.Contains(err.Error(), " did exist") {
		t.Fatal(err)
	}
}
