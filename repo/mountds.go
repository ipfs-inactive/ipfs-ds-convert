package repo

import (
	"fmt"
	"sort"

	ds "github.com/ipfs/go-datastore"
	mount "github.com/ipfs/go-datastore/mount"
)

type mountDatastoreConfig struct {
	mounts []premount
}

type premount struct {
	ds     DatastoreConfig
	prefix ds.Key
}

// MountDatastoreConfig returns a mount DatastoreConfig from a spec
func MountDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var res mountDatastoreConfig
	mounts, ok := params["mounts"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("'mounts' field is missing or not an array")
	}
	for _, iface := range mounts {
		cfg, ok := iface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected map for mountpoint")
		}

		child, err := AnyDatastoreConfig(cfg)
		if err != nil {
			return nil, err
		}

		prefix, found := cfg["mountpoint"]
		if !found {
			return nil, fmt.Errorf("no 'mountpoint' on mount")
		}

		res.mounts = append(res.mounts, premount{
			ds:     child,
			prefix: ds.NewKey(prefix.(string)),
		})
	}
	sort.Slice(res.mounts,
		func(i, j int) bool {
			return res.mounts[i].prefix.String() > res.mounts[j].prefix.String()
		})

	return &res, nil
}

func (c *mountDatastoreConfig) DiskSpec() DiskSpec {
	cfg := map[string]interface{}{"type": "mount"}
	mounts := make([]interface{}, len(c.mounts))
	for i, m := range c.mounts {
		c := m.ds.DiskSpec()
		if c == nil {
			c = make(map[string]interface{})
		}
		c["mountpoint"] = m.prefix.String()
		mounts[i] = c
	}
	cfg["mounts"] = mounts
	return cfg
}

func (c *mountDatastoreConfig) Create(path string) (Datastore, error) {
	mounts := make([]mount.Mount, len(c.mounts))
	for i, m := range c.mounts {
		ds, err := m.ds.Create(path)
		if err != nil {
			return nil, err
		}
		mounts[i].Datastore = ds
		mounts[i].Prefix = m.prefix
	}
	return mount.New(mounts), nil
}
