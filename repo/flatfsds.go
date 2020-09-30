package repo

import (
	"fmt"
	"path/filepath"

	flatfs "github.com/ipfs/go-ds-flatfs"
)

type flatfsDatastoreConfig struct {
	path      string
	shardFun  *flatfs.ShardIdV1
	syncField bool
}

// FlatfsDatastoreConfig returns a flatfs DatastoreConfig from a spec
func FlatfsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c flatfsDatastoreConfig
	var ok bool
	var err error

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not boolean")
	}

	sshardFun, ok := params["shardFunc"].(string)
	if !ok {
		return nil, fmt.Errorf("'shardFunc' field is missing or not a string")
	}
	c.shardFun, err = flatfs.ParseShardFunc(sshardFun)
	if err != nil {
		return nil, err
	}

	c.syncField, ok = params["sync"].(bool)
	if !ok {
		return nil, fmt.Errorf("'sync' field is missing or not boolean")
	}
	return &c, nil
}

func (c *flatfsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type":      "flatfs",
		"path":      c.path,
		"shardFunc": c.shardFun.String(),
	}
}

func (c *flatfsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return flatfs.CreateOrOpen(p, c.shardFun, c.syncField)
}
