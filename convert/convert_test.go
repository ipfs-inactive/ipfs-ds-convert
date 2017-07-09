package convert_test

import (
	"path"
	"testing"

	convert "github.com/ipfs/ipfs-ds-convert/convert"
	testutil "github.com/ipfs/ipfs-ds-convert/testutil"
)

func TestBasicConvert(t *testing.T) {
	//Prepare repo
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

	err = testutil.PatchConfig(path.Join(dir, "config"), "testfiles/badgerSpec")
	if err != nil {
		t.Fatal(err)
	}

	//Convert!
	err = convert.Convert(dir)
	if err != nil {
		t.Fatal(err)
	}

	//Test if repo can be opened
	r, err = testutil.OpenRepo(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
