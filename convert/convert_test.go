package convert_test

import (
	"path"
	"testing"

	convert "github.com/ipfs/ipfs-ds-convert/convert"
	testutil "github.com/ipfs/ipfs-ds-convert/testutil"
)

func prepareTest(t *testing.T, keys, blocks int) (string, func(t *testing.T), int64, int64) {
	dir, _close := testutil.NewTestRepo(t)

	r, err := testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	seed1, err := testutil.InsertRandomKeys("", keys, r)
	if err != nil {
		t.Fatal(err)
	}

	seed2, err := testutil.InsertRandomKeys("blocks/", blocks, r)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	return dir, _close, seed1, seed2
}

func finishTest(t *testing.T, dir string, seed1, seed2 int64, keys, blocks int) {
	//Test if repo can be opened
	r, err := testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = testutil.Verify("", keys, seed1, r)
	if err != nil {
		t.Fatal(err)
	}

	err = testutil.Verify("blocks/", blocks, seed2, r)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicConvert(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := prepareTest(t, 3000, 10000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir)
	if err != nil {
		t.Fatal(err)
	}

	finishTest(t, dir, s1, s2, 3000, 10000)
}
