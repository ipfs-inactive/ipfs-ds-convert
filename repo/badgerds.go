package repo

import (
	"fmt"
	"os"
	"path/filepath"

	badgerds "github.com/ipfs/go-ds-badger"
)

type badgerdsDatastoreConfig struct {
	path       string
	syncWrites bool
}

// BadgerdsDatastoreConfig returns a configuration stub for a badger datastore
// from the given parameters
func BadgerdsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c badgerdsDatastoreConfig
	var ok bool

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	sw, ok := params["syncWrites"]
	if !ok {
		c.syncWrites = true
	} else {
		if swb, ok := sw.(bool); ok {
			c.syncWrites = swb
		} else {
			return nil, fmt.Errorf("'syncWrites' field was not a boolean")
		}
	}

	return &c, nil
}

func (c *badgerdsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "badgerds",
		"path": c.path,
	}
}

func (c *badgerdsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	err := os.MkdirAll(p, 0755)
	if err != nil {
		return nil, err
	}

	defopts := badgerds.DefaultOptions
	defopts.SyncWrites = c.syncWrites

	return badgerds.NewDatastore(p, &defopts)
}
