package repo

import (
	"errors"
	"fmt"
	"path/filepath"

	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

type leveldsDatastoreConfig struct {
	path        string
	compression ldbopts.Compression
}

// LeveldsDatastoreConfig returns a levelds DatastoreConfig from a spec
func LeveldsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c leveldsDatastoreConfig
	var ok bool

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, errors.New("'path' field is missing or not string")
	}

	switch cm := params["compression"].(string); cm {
	case "none":
		c.compression = ldbopts.NoCompression
	case "snappy":
		c.compression = ldbopts.SnappyCompression
	case "":
		c.compression = ldbopts.DefaultCompression
	default:
		return nil, fmt.Errorf("unrecognized value for compression: %s", cm)
	}

	return &c, nil
}

func (c *leveldsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "levelds",
		"path": c.path,
	}
}

func (c *leveldsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return levelds.NewDatastore(p, &levelds.Options{
		Compression: c.compression,
	})
}
