package repo

import (
	ds "github.com/ipfs/go-datastore"
)

type memDatastoreConfig struct {
	cfg map[string]interface{}
}

// MemDatastoreConfig returns a memory DatastoreConfig from a spec
func MemDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	return &memDatastoreConfig{params}, nil
}

func (c *memDatastoreConfig) DiskSpec() DiskSpec {
	return nil
}

func (c *memDatastoreConfig) Create(string) (Datastore, error) {
	return ds.NewMapDatastore(), nil
}

type measureDatastoreConfig struct {
	child  DatastoreConfig
	prefix string
}
