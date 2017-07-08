package main_test

import (
	"testing"

	testutil "github.com/ipfs/ipfs-ds-convert/testutil"
	convert "github.com/ipfs/ipfs-ds-convert"
)

func TestBasicConvert(t *testing.T) {
	dir, _close := testutil.NewTestRepo(t)
	defer _close(t)

	r, err := testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = testutil.InsertRandomKeys("", 50, r)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = convert.Convert(dir, "testfiles/badgerSpec")
	if err != nil {
		t.Fatal(err)
	}
}

func TestLargeConvert(t *testing.T) {
	dir, _close := testutil.NewTestRepo(t)
	defer _close(t)

	r, err := testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = testutil.InsertRandomKeys("", 10000, r)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = convert.Convert(dir, "testfiles/badgerSpec")
	if err != nil {
		t.Fatal(err)
	}
}
