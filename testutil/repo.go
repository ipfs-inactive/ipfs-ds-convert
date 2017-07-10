package testutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	convert "github.com/ipfs/ipfs-ds-convert/convert"
	config "gx/ipfs/QmTfEA66H8JVypARWoQuQv3bw38DXjxtMZpcEKctpjnz3V/go-ipfs/repo/config"
	fsrepo "gx/ipfs/QmTfEA66H8JVypARWoQuQv3bw38DXjxtMZpcEKctpjnz3V/go-ipfs/repo/fsrepo"
)

func NewTestRepo(t *testing.T) (string, func(t *testing.T)) {
	conf, err := config.Init(os.Stdout, 1024)
	if err != nil {
		t.Fatal(err)
	}

	err = config.ConfigProfiles["test"](conf)
	if err != nil {
		t.Fatal(err)
	}

	repoRoot, err := ioutil.TempDir("/tmp", "ds-convert-test-")
	if err != nil {
		t.Fatal(err)
	}

	if err := fsrepo.Init(repoRoot, conf); err != nil {
		t.Fatal(err)
	}

	return repoRoot, func(t *testing.T) {
		err := os.RemoveAll(repoRoot)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func PatchConfig(t *testing.T, configPath string, newSpecPath string) {
	newSpec := make(map[string]interface{})
	err := convert.LoadConfig(newSpecPath, &newSpec)
	if err != nil {
		t.Fatal(err)
	}

	repoConfig := make(map[string]interface{})
	err = convert.LoadConfig(configPath, &repoConfig)
	if err != nil {
		t.Fatal(err)
	}

	dsConfig, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		t.Fatal(fmt.Errorf("no 'Datastore' or invalid type in %s", configPath))
	}

	_, ok = dsConfig["Spec"].(map[string]interface{})
	if !ok {
		t.Fatal(fmt.Errorf("no 'Datastore.Spec' or invalid type in %s", configPath))
	}

	dsConfig["Spec"] = newSpec

	b, err := json.MarshalIndent(repoConfig, "", "  ")
	ioutil.WriteFile(configPath, b, 0660)
}
