package convert_test

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/config"

	convert "github.com/ipfs/ipfs-ds-convert/convert"
	testutil "github.com/ipfs/ipfs-ds-convert/testutil"
)

func TestBasicConvert(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := testutil.PrepareTest(t, 3000, 3000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/badgerSpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	testutil.FinishTest(t, dir, s1, s2, 3000, 3000)
}

func TestLossyConvert(t *testing.T) {
	//Prepare repo
	dir, _close, _, _ := testutil.PrepareTest(t, 100, 100)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/lossySpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err != nil {
		if !strings.Contains(err.Error(), "adding missing to src spec: couldn't find best match for specA /") {
			t.Fatal(err)
		}
		return
	}

	t.Errorf("expected error 'adding missing to src spec: couldn't find best match for specA /'")
}

// should cover noop case in convert.go
func TestNoopConvert(t *testing.T) {
	//Prepare repo
	dir, _close, s1, s2 := testutil.PrepareTest(t, 3000, 3000)
	defer _close(t)

	testutil.PatchConfig(t, path.Join(dir, "config"), "../testfiles/equalSpec")

	//Convert!
	err := convert.Convert(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	testutil.FinishTest(t, dir, s1, s2, 3000, 3000)
}

func TestSkipCopyConvert(t *testing.T) {
	spec := make(map[string]interface{})
	err := config.Load("../testfiles/skipableSpec", &spec)
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

	err = convert.Convert(dir, false)
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
