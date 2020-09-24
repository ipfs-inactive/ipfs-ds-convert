package repo

import (
	"fmt"

	ds "github.com/ipfs/go-datastore"
)

type logDatastoreConfig struct {
	child DatastoreConfig
	name  string
}

// LogDatastoreConfig returns a log DatastoreConfig from a spec
func LogDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	childField, ok := params["child"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'child' field is missing or not a map")
	}
	child, err := AnyDatastoreConfig(childField)
	if err != nil {
		return nil, err
	}
	name, ok := params["name"].(string)
	if !ok {
		return nil, fmt.Errorf("'name' field was missing or not a string")
	}
	return &logDatastoreConfig{child, name}, nil

}

func (c *logDatastoreConfig) Create(path string) (Datastore, error) {
	child, err := c.child.Create(path)
	if err != nil {
		return nil, err
	}
	return ds.NewLogDatastore(child, c.name), nil
}

func (c *logDatastoreConfig) DiskSpec() DiskSpec {
	return c.child.DiskSpec()
}
