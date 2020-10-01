package testutil

import (
	"fmt"
	"testing"
)

func PrepareTest(t *testing.T, keys, blocks int) (string, func(t *testing.T), int64, int64) {
	return PrepareTestSpec(t, keys, blocks, nil)
}

func PrepareTestSpec(t *testing.T, keys, blocks int, spec map[string]interface{}) (string, func(t *testing.T), int64, int64) {
	dir, _close := NewTestRepo(t, spec)

	r, err := OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	seed1, err := InsertRandomKeys("", keys, r)
	if err != nil {
		t.Fatal(err)
	}

	seed2, err := InsertRandomKeys("blocks/", blocks, r)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	return dir, _close, seed1, seed2
}

func FinishTest(t *testing.T, dir string, seed1, seed2 int64, keys, blocks int) {
	//Test if repo can be opened
	r, err := OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Verifying keys")
	err = Verify("", keys, seed1, r)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Verifying blocks")
	err = Verify("blocks/", blocks, seed2, r)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
