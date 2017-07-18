package convert

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ipfs/ipfs-ds-convert/config"

	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
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
	specStat, err := os.Stat(filepath.Join(c.path, SpecsFile))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if specStat.Mode()&0200 == 0 {
		return errors.New("datastore_spec is not writable")
	}

	oldSpec := make(map[string]interface{})
	err = LoadConfig(filepath.Join(c.path, SpecsFile), &oldSpec)
	if err != nil {
		return err
	}

	_, err = config.Validate(oldSpec, true)
	if err != nil {
		return err
	}

	c.fromSpec = oldSpec

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

	c.toSpec = dsSpec
	return nil
}
