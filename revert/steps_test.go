package revert

import (
	"os"
	"path"
	"strings"
	"testing"
)

func TestLoadNonexistentLog(t *testing.T) {
	_, err := loadLog(path.Join(os.TempDir(), "non/existent/path"))
	if !os.IsNotExist(err) {
		t.Errorf("unexpected error %s, expected no such file or directory", err)
	}
}

func TestLoadInvalidLog(t *testing.T) {
	dname, _ := os.MkdirTemp(os.TempDir(), "ds-convert-test-")
	_ = os.WriteFile(path.Join(dname, ConvertLog), []byte("not a json\n"), 0600)

	_, err := loadLog(dname)
	if !strings.Contains(err.Error(), "invalid character 'o' in literal null (expecting 'u')") {
		t.Errorf("unexpected error %s, expected invalid character...", err)
	}

	_ = os.WriteFile(path.Join(dname, ConvertLog), []byte(`{"action":5}`), 0600)

	_, err = loadLog(dname)
	if !strings.Contains(err.Error(), "invalid action type in convert steps") {
		t.Errorf("unexpected error %s, expected invalid action type in convert steps", err)
	}

	_ = os.WriteFile(path.Join(dname, ConvertLog), []byte(`{"action":"a","arg":[3]}`), 0600)

	_, err = loadLog(dname)
	if !strings.Contains(err.Error(), "invalid arg 0 in convert steps") {
		t.Errorf("unexpected error %s, expected invalid arg 0 in convert steps", err)
	}

	os.Remove(dname)
}
