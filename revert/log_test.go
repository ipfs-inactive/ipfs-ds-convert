package revert_test

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/revert"
)

func TestNewActionLogger(t *testing.T) {
	d, err := os.MkdirTemp(os.TempDir(), "ds-convert-test-")
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(path.Join(d, revert.ConvertLog), []byte{}, 0664)
	if err != nil {
		t.Fatal(err)
	}

	_, err = revert.NewActionLogger(d)
	if !strings.Contains(err.Error(), "convertlog already exists, you may want to run revert") {
		t.Fatalf("expected error, got %s", err)
	}

	err = os.RemoveAll(d)
	if err != nil {
		t.Fatal(err)
	}

	_, err = revert.NewActionLogger(path.Join(d, "non/existent/path"))
	if !strings.Contains(err.Error(), "/non/existent/path/convertlog:") {
		t.Fatalf("expected error, got %s", err)
	}
}

func TestLog(t *testing.T) {
	d, err := os.MkdirTemp(os.TempDir(), "ds-convert-test-")
	if err != nil {
		t.Fatal(err)
	}
	//defer os.RemoveAll(d)

	lg, err := revert.NewActionLogger(d)
	if err != nil {
		t.Fatal(err)
	}

	err = lg.Log("abc", "def")
	if err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(path.Join(d, revert.ConvertLog))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(b), `{"action":"abc","arg":["def"]}`) {
		t.Errorf("unexpected revert log, got: `%s`", string(b))
	}
}
