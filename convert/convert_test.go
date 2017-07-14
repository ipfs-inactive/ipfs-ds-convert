package convert_test

import (
	"fmt"
	"path"
	"testing"

	convert "github.com/ipfs/ipfs-ds-convert/convert"
	testutil "github.com/ipfs/ipfs-ds-convert/testutil"
)

func prepareTest(t *testing.T, keys, blocks int) (string, func(t *testing.T), int64, int64) {
	dir, _close := testutil.NewTestRepo(t, nil)

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

	fmt.Println("Generating keys")
	err = testutil.Verify("", keys, seed1, r)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Generating blocks")
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
	dir, _close, s1, s2 := prepareTest(t, 3000, 3000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir)
	if err != nil {
		t.Fatal(err)
	}

	finishTest(t, dir, s1, s2, 3000, 3000)
}

func TestSkipCopyConvert(t *testing.T) {
	spec := make(map[string]interface{})
	err := convert.LoadConfig("../testfiles/skipableSpec", &spec)
	if err != nil {
		t.Fatal(err)
	}

	dir, _close := testutil.NewTestRepo(t, spec)
	defer _close(t)

	r, err := testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	prefixes := []string{"a/", "b/", "c/", "d/", "e/"}
	seeds := []int64{}

	for _, prefix := range prefixes {
		fmt.Println("Generating " + prefix)
		seed, err := testutil.InsertRandomKeys(prefix, 1000, r)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, seed)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/skipableDstSpec")

	err = convert.Convert(dir)
	if err != nil {
		t.Fatal(err)
	}

	r, err = testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i, prefix := range prefixes {
		err = testutil.Verify(prefix, 1000, seeds[i], r)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
