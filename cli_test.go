package main

import (
	"path"
	"testing"

	"os"

	"github.com/ipfs/ipfs-ds-convert/testutil"
)

func TestBasicConvert(t *testing.T) {
	dir, _close, s1, s2 := testutil.PrepareTest(t, 2000, 2000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "testfiles/badgerSpec")

	os.Setenv(EnvDir, dir)
	run([]string{".", "convert"})

	testutil.FinishTest(t, dir, s1, s2, 2000, 2000)
}

func TestBasicRevert(t *testing.T) {
	dir, _close, s1, s2 := testutil.PrepareTest(t, 200, 200)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "testfiles/badgerSpec")

	os.Setenv(EnvDir, dir)
	run([]string{".", "convert", "--keep"})

	testutil.FinishTest(t, dir, s1, s2, 200, 200)

	os.Setenv(EnvDir, dir)
	run([]string{".", "revert", "--force", "--fix-config"})

	testutil.FinishTest(t, dir, s1, s2, 200, 200)
}

func TestBasicCleanup(t *testing.T) {
	dir, _close, s1, s2 := testutil.PrepareTest(t, 200, 200)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "testfiles/badgerSpec")

	os.Setenv(EnvDir, dir)
	run([]string{".", "convert", "--keep"})

	testutil.FinishTest(t, dir, s1, s2, 200, 200)

	os.Setenv(EnvDir, dir)
	run([]string{".", "cleanup"})

	testutil.FinishTest(t, dir, s1, s2, 200, 200)
}
