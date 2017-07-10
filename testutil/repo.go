package testutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	config "github.com/ipfs/go-ipfs/repo/config"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	convert "github.com/ipfs/ipfs-ds-convert/convert"
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

func PatchConfig(configPath string, newSpecPath string) error {
	newSpec := make(map[string]interface{})
	err := convert.LoadConfig(newSpecPath, &newSpec)
	if err != nil {
		return err
	}

	repoConfig := make(map[string]interface{})
	err = convert.LoadConfig(configPath, &repoConfig)
	if err != nil {
		return err
	}

	dsConfig, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore' or invalid type in %s", configPath)
	}

	_, ok = dsConfig["Spec"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore.Spec' or invalid type in %s", configPath)
	}

	dsConfig["Spec"] = newSpec

	b, err := json.MarshalIndent(repoConfig, "", "  ")
	ioutil.WriteFile(configPath, b, 0660)

	return nil
}
