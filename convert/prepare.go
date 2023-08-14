package convert

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ipfs/ipfs-ds-convert/config"
	"github.com/ipfs/ipfs-ds-convert/repo"

	"github.com/pkg/errors"
)

func (c *Conversion) checkRepoVersion() error {
	vstr, err := os.ReadFile(filepath.Join(c.path, "version"))
	if err != nil {
		return err
	}

	version, err := strconv.Atoi(strings.TrimSpace(string(vstr)))
	if err != nil {
		return err
	}

	if version != repo.SupportedRepoVersion {
		return fmt.Errorf("unsupported fsrepo version: %d", version)
	}

	return nil
}

func (c *Conversion) loadSpecs() error {
	specStat, err := os.Stat(filepath.Join(c.path, repo.SpecsFile))
	if os.IsNotExist(err) {
		return err
	}

	if specStat.Mode()&0200 == 0 {
		return errors.New("datastore_spec is not writable")
	}

	oldSpec := make(map[string]interface{})
	err = config.Load(filepath.Join(c.path, repo.SpecsFile), &oldSpec)
	if err != nil {
		return err
	}

	_, err = config.Validate(oldSpec, true)
	if err != nil {
		return errors.Wrapf(err, "validating datastore_spec spec")
	}

	c.fromSpec = oldSpec

	repoConfig := make(map[string]interface{})
	err = config.Load(filepath.Join(c.path, repo.ConfigFile), &repoConfig)
	if err != nil {
		return err
	}

	dsConfig, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore' or invalid type in %s", filepath.Join(c.path, repo.ConfigFile))
	}

	dsSpec, ok := dsConfig["Spec"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore.Spec' or invalid type in %s", filepath.Join(c.path, repo.ConfigFile))
	}

	_, err = config.Validate(dsSpec, false)
	if err != nil {
		return errors.Wrapf(err, "validating new spec")
	}

	c.toSpec = dsSpec
	return nil
}
