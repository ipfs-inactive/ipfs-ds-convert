package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ipfs/ipfs-ds-convert/config"
	"github.com/pkg/errors"
)

const SuppertedRepoVersion = 6

// conversion holds conversion state and progress
type conversion struct {
	steps []string

	path string
	newDsDir string

	dsSpec map[string]interface{}
	newDsSpec map[string]interface{}

	oldDs Datastore
	newDs Datastore
}

func Convert(repoPath string, newConfigPath string) error {
	c := conversion{
		steps: []string{},

		path: repoPath,
	}

	err := c.checkRepoVersion()
	if err != nil {
		return err
	}

	err = c.loadSpecs(newConfigPath)
	if err != nil {
		return err
	}

	err = c.validateSpecs(newConfigPath)
	if err != nil {
		return err
	}

	err = c.openDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	// Copy keys

	err = c.closeDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	// move old to another temp

	// move new to repo

	// open repos

	// verify keys(/whole data?) integrity (opt-out)

	// close repos

	// transform config

	// check config

	return nil
}

func loadConfig(path string, out *map[string]interface{}) error {
	cfgbytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = json.Unmarshal(cfgbytes, out)
	if err != nil {
		return err
	}

	return nil
}

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
		return fmt.Errorf("Unsupperted fsrepo version: %d", version)
	}

	return nil
}

func (c *conversion) loadSpecs(newConfigPath string) error {
	c.newDsSpec = make(map[string]interface{})
	err := loadConfig(newConfigPath, &c.newDsSpec)
	if err != nil {
		return err
	}

	repoConfig := make(map[string]interface{})
	err = loadConfig(filepath.Join(c.path, "config"), &repoConfig)
	if err != nil {
		return err
	}

	dsConfig, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore' or invalid type in %s", filepath.Join(c.path, "config"))
	}

	dsSpec, ok := dsConfig["Spec"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore.Spec' or invalid type in %s", filepath.Join(c.path, "config"))
	}

	c.dsSpec = dsSpec
	return nil
}

func (c *conversion) validateSpecs(newConfigPath string) error {
	err := config.Validate(c.dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", filepath.Join(c.path, "config"))
	}

	err = config.Validate(c.newDsSpec)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", newConfigPath)
	}

	return nil
}

func (c *conversion) openDatastores() (err error) {
	c.oldDs, err = OpenDatastore(c.path, c.dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening datastore at %s", c.path)
	}
	c.addStep("open datastore at %s", c.path)

	c.newDsDir, err = ioutil.TempDir(c.path, "ds-convert")
	if err != nil {
		return errors.Wrapf(err, "error creating temp datastore at %s", c.path)
	}
	c.addStep("create temp datastore directory at %s", c.newDsDir)

	c.newDs, err = OpenDatastore(c.newDsDir, c.newDsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening new datastore at %s", c.newDsDir)
	}
	c.addStep("open new datastore at %s", c.newDsDir)

	return nil
}

func (c *conversion) closeDatastores() error {
	err := c.oldDs.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing datastore at %s", c.path)
	}
	c.addStep("close datastore at %s", c.path)

	err = c.newDs.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing new datastore at %s", c.newDsDir)
	}
	c.addStep("close new datastore at %s", c.newDsDir)
	return nil
}

func (c *conversion) addStep(format string, args ...interface{}) {
	c.steps = append(c.steps, fmt.Sprintf(format, args))
}

func (c *conversion) wrapErr(err error) error {
	s := strings.Join(c.steps, "\n")

	return errors.Wrapf(err, "Conversion steps done so far:\n%s", s)
}
