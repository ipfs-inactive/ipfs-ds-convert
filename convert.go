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

func checkRepoVersion(path string) error {
	vstr, err := ioutil.ReadFile(filepath.Join(path, "version"))
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

func Convert(repoPath string, newConfigPath string) error {
	steps := []string{}

	err := checkRepoVersion(repoPath)
	if err != nil {
		return err
	}

	// Parse config

	repoConfig := make(map[string]interface{})
	err = loadConfig(filepath.Join(repoPath, "config"), &repoConfig)
	if err != nil {
		return err
	}

	newDsSpec := make(map[string]interface{})
	err = loadConfig(newConfigPath, &newDsSpec)
	if err != nil {
		return err
	}

	dsConfig, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore' or invalid type in %s", filepath.Join(repoPath, "config"))
	}

	dsSpec, ok := dsConfig["Spec"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("no 'Datastore.Spec' or invalid type in %s", filepath.Join(repoPath, "config"))
	}

	// Validate config

	err = config.Validate(dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", filepath.Join(repoPath, "config"))
	}

	err = config.Validate(newDsSpec)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", newConfigPath)
	}

	// Open/prepare datastores
	oldDs, err := OpenDatastore(repoPath, dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening datastore at %s", repoPath)
	}
	defer func() {
		oldDs.Close() //TODO: check error?
	}()
	steps = append(steps, fmt.Sprintf("open datastore at %s", repoPath))

	newDsDir, err := ioutil.TempDir(repoPath, "ds-convert")
	if err != nil {
		return wrapErr(err, steps,"error creating temp datastore at %s", repoPath)
	}
	steps = append(steps, fmt.Sprintf("create temp datastore directory at %s", newDsDir))

	newDs, err := OpenDatastore(newDsDir, newDsSpec)
	if err != nil {
		return wrapErr(err, steps,"error opening new datastore at %s", newDsDir)
	}
	defer func() {
		newDs.Close() //TODO: check error?
	}()
	steps = append(steps, fmt.Sprintf("open new datastore at %s", newDsDir))




	return nil
}

func wrapErr(err error, steps []string, format string, args ...interface{}) error {
	s := strings.Join(steps, "\n")

	return errors.Wrapf(err, format + "\nConversion steps done so far:\n%s", append(args, s))
}
