package convert

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"

	config "github.com/ipfs/ipfs-ds-convert/config"
	errors "gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
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

	c.newDsSpec = dsSpec
	return nil
}

func (c *conversion) validateSpecs() error {
	oldPaths, err := config.Validate(c.dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", filepath.Join(c.path, SpecsFile))
	}
	c.oldPaths = oldPaths

	newPaths, err := config.Validate(c.newDsSpec)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", filepath.Join(c.path, ConfigFile))
	}
	c.newPaths = newPaths

	return nil
}
