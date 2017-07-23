package revert_test

import (
	"path"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"github.com/ipfs/ipfs-ds-convert/testutil"
	"github.com/ipfs/ipfs-ds-convert/revert"
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

	err = revert.Revert(dir, true, false)
	if err != nil {
		t.Fatal(err)
	}

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/defaultSpec")

	testutil.FinishTest(t, dir, s1, s2, 1000, 1000)
}

func TestBasicConvertCleanup(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := testutil.PrepareTest(t, 1000, 1000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	err = revert.Revert(dir, true, true)
	if err != nil {
		t.Fatal(err)
	}

	testutil.FinishTest(t, dir, s1, s2, 1000, 1000)
}
