package repo

import (
	"fmt"
	"os"
	"path/filepath"

	badger2ds "github.com/ipfs/go-ds-badger2"
)

type badger2dsDatastoreConfig struct {
	path       string
	syncWrites bool
}

// Badger2dsDatastoreConfig returns a configuration stub for a badger2
// datastore from the given parameters
func Badger2dsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c badger2dsDatastoreConfig
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

func (c *badger2dsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "badger2ds",
		"path": c.path,
	}
}

func (c *badger2dsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	err := os.MkdirAll(p, 0755)
	if err != nil {
		return nil, err
	}

	defopts := badger2ds.DefaultOptions
	defopts.SyncWrites = c.syncWrites

	return badger2ds.NewDatastore(p, &defopts)
}
