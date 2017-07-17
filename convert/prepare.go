package convert

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"github.com/ipfs/ipfs-ds-convert/config"
)

func (c *conversion) checkRepoVersion() error {
	vstr, err := ioutil.ReadFile(filepath.Join(c.path, "version"))
	if err != nil {
		return err
	}

	version, err := strconv.Atoi(strings.TrimSpace(string(vstr)))
	if err != nil {
		return err
	}

	if version != SuppertedRepoVersion {
		return fmt.Errorf("unsupported fsrepo version: %d", version)
	}

	return nil
}

func (c *conversion) loadSpecs() error {
	oldSpec := make(map[string]interface{})
	err := LoadConfig(filepath.Join(c.path, SpecsFile), &oldSpec)
	if err != nil {
		return err
	}

	_, err = config.Validate(oldSpec, true)
	if err != nil {
		return err
	}

	c.dsSpec = oldSpec

	repoConfig := make(map[string]interface{})
	err = LoadConfig(filepath.Join(c.path, ConfigFile), &repoConfig)
	if err != nil {
		return err
	}

	dsConfig, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore' or invalid type in %s", filepath.Join(c.path, ConfigFile))
	}

	dsSpec, ok := dsConfig["Spec"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore.Spec' or invalid type in %s", filepath.Join(c.path, ConfigFile))
	}

	_, err = config.Validate(dsSpec, false)
	if err != nil {
		return err
	}

	c.newDsSpec = dsSpec
	return nil
}
