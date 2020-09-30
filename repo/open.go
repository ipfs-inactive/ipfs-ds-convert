package repo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	ds "github.com/ipfs/go-datastore"
	retry "github.com/ipfs/go-datastore/retrystore"
)

//TODO: extract and use fsrepo from go-ipfs

type Datastore interface {
	ds.Batching
}

type Retry struct {
	*retry.Datastore
	io.Closer
}

func (ds *Retry) Close() error {
	return ds.Batching.(Datastore).Close()
}

func isTooManyFDError(err error) bool {
	perr, ok := err.(*os.PathError)
	if ok && perr.Err == syscall.EMFILE {
		return true
	}

	return false
}

func OpenDatastore(path string, params map[string]interface{}) (Datastore, error) {
	dsc, err := AnyDatastoreConfig(params)
	if err != nil {
		return nil, err
	}

	d, err := dsc.Create(path)
	if err != nil {
		return nil, err
	}

	rds := &retry.Datastore{
		Batching:    d,
		Delay:       time.Millisecond * 200,
		Retries:     6,
		TempErrFunc: isTooManyFDError,
	}

	return &Retry{
		Datastore: rds,
		Closer:    d,
	}, nil
}

func DatastoreSpec(params map[string]interface{}) (string, error) {
	dsc, err := AnyDatastoreConfig(params)
	if err != nil {
		return "", err
	}
	return dsc.DiskSpec().String(), nil
}

// From https://github.com/ipfs/go-ipfs/blob/8525be5990d3a0b5ece0d6773764756f9cbf15e9/repo/fsrepo/datastores.go
// ConfigFromMap creates a new datastore config from a map
type ConfigFromMap func(map[string]interface{}) (DatastoreConfig, error)

// DatastoreConfig is an abstraction of a datastore config.  A "spec"
// is first converted to a DatastoreConfig and then Create() is called
// to instantiate a new datastore
type DatastoreConfig interface {
	// DiskSpec returns a minimal configuration of the datastore
	// represting what is stored on disk.  Run time values are
	// excluded.
	DiskSpec() DiskSpec

	// Create instantiate a new datastore from this config
	Create(path string) (Datastore, error)
}

// DiskSpec is the type returned by the DatastoreConfig's DiskSpec method
type DiskSpec map[string]interface{}

// Bytes returns a minimal JSON encoding of the DiskSpec
func (spec DiskSpec) Bytes() []byte {
	b, err := json.Marshal(spec)
	if err != nil {
		// should not happen
		panic(err)
	}
	return bytes.TrimSpace(b)
}

// String returns a minimal JSON encoding of the DiskSpec
func (spec DiskSpec) String() string {
	return string(spec.Bytes())
}

var datastores map[string]ConfigFromMap

func init() {
	datastores = map[string]ConfigFromMap{
		"mount":     MountDatastoreConfig,
		"flatfs":    FlatfsDatastoreConfig,
		"levelds":   LeveldsDatastoreConfig,
		"badgerds":  BadgerdsDatastoreConfig,
		"badger2ds": Badger2dsDatastoreConfig,
		"mem":       MemDatastoreConfig,
		"log":       LogDatastoreConfig,
		"measure":   MeasureDatastoreConfig,
	}
}

// AnyDatastoreConfig returns a DatastoreConfig from a spec based on
// the "type" parameter
func AnyDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	which, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("'type' field missing or not a string")
	}
	fun, ok := datastores[which]
	if !ok {
		return nil, fmt.Errorf("unknown datastore type: %s", which)
	}
	return fun(params)
}
