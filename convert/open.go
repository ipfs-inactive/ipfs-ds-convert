package convert

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	measure "gx/ipfs/QmSb95iHExSSb47zpmyn5CyY5PZidVWSjyKyDqgYQrnKor/go-ds-measure"
	flatfs "gx/ipfs/QmUTshC2PP4ZDqkrFfDU4JGJFMWjYnunxPgkQ6ZCA2hGqh/go-ds-flatfs"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	mount "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/syncmount"

	retry "gx/ipfs/QmPP91WFAb8LCs8EMzGvDPPvg1kacbqRkoxgTTnUsZckGe/retry-datastore"
	levelds "gx/ipfs/QmPdvXuXWAR6gtxxqZw42RtSADMwz4ijVmYHGS542b6cMz/go-ds-leveldb"
	badgerds "gx/ipfs/QmTC7BY2viSAbPbGte6NyMZJCC2xheaRBYKEn4BuzTxe7W/go-ds-badger"
	ldbopts "gx/ipfs/QmbBhyDKsY4mbY6xsKt3qu9Y7FPvMJ6qbD8AMjYYvPRw1g/goleveldb/leveldb/opt"
)

//TODO: extract and use fsrepo from go-ipfs

type Datastore interface {
	ds.Batching
	io.Closer
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

func DatastoreId(params map[string]interface{}) string {
	dsc, err := AnyDatastoreConfig(params)
	if err != nil {
		return ""
	}
	return dsc.DiskId()
}

// From https://github.com/ipfs/go-ipfs/blob/8525be5990d3a0b5ece0d6773764756f9cbf15e9/repo/fsrepo/datastores.go

// ConfigFromMap creates a new datastore config from a map
type ConfigFromMap func(map[string]interface{}) (DatastoreConfig, error)

type DatastoreConfig interface {
	// DiskId is a unique id representing the Datastore config as
	// stored on disk, runtime config values are not part of this Id.
	// Returns an empty string if the datastore does not have an on
	// disk representation. No length limit.
	DiskId() string

	// Create instantiate a new datastore from this config
	Create(path string) (Datastore, error)
}

var datastores map[string]ConfigFromMap

func init() {
	datastores = map[string]ConfigFromMap{
		"mount":    MountDatastoreConfig,
		"flatfs":   FlatfsDatastoreConfig,
		"levelds":  LeveldsDatastoreConfig,
		"badgerds": BadgerdsDatastoreConfig,
		"log":      LogDatastoreConfig,
		"measure":  MeasureDatastoreConfig,
	}
}

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

type mountDatastoreConfig struct {
	mounts []premount
}

type premount struct {
	ds     DatastoreConfig
	prefix ds.Key
}

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

	return &res, nil
}

func (c *mountDatastoreConfig) DiskId() string {
	buf := new(bytes.Buffer)
	for _, m := range c.mounts {
		fmt.Fprintf(buf, "%s:{%s};", m.prefix.String(), m.ds.DiskId())
	}
	return buf.String()
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

type flatfsDatastoreConfig struct {
	path      string
	shardFun  *flatfs.ShardIdV1
	syncField bool
}

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

func (c *flatfsDatastoreConfig) DiskId() string {
	return fmt.Sprintf("flatfs;%s;%s", c.path, c.shardFun.String())
}

func (c *flatfsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return flatfs.CreateOrOpen(p, c.shardFun, c.syncField)
}

type leveldsDatastoreConfig struct {
	path        string
	compression ldbopts.Compression
}

func LeveldsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c leveldsDatastoreConfig
	var ok bool

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	switch params["compression"].(string) {
	case "none":
		c.compression = ldbopts.NoCompression
	case "snappy":
		c.compression = ldbopts.SnappyCompression
	case "":
		fallthrough
	default:
		c.compression = ldbopts.DefaultCompression
	}

	return &c, nil
}

func (c *leveldsDatastoreConfig) DiskId() string {
	return fmt.Sprintf("levelds;%s", c.path)
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

type logDatastoreConfig struct {
	child DatastoreConfig
	name  string
}

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

func (c *logDatastoreConfig) DiskId() string {
	return c.child.DiskId()
}

type measureDatastoreConfig struct {
	child  DatastoreConfig
	prefix string
}

func MeasureDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	childField, ok := params["child"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'child' field is missing or not a map")
	}
	child, err := AnyDatastoreConfig(childField)
	if err != nil {
		return nil, err
	}
	prefix, ok := params["prefix"].(string)
	if !ok {
		return nil, fmt.Errorf("'prefix' field was missing or not a string")
	}
	return &measureDatastoreConfig{child, prefix}, nil
}

func (c *measureDatastoreConfig) DiskId() string {
	return c.child.DiskId()
}

func (c measureDatastoreConfig) Create(path string) (Datastore, error) {
	child, err := c.child.Create(path)
	if err != nil {
		return nil, err
	}
	return measure.New(c.prefix, child), nil
}

type badgerdsDatastoreConfig struct {
	path string
}

func BadgerdsDatastoreConfig(params map[string]interface{}) (DatastoreConfig, error) {
	var c badgerdsDatastoreConfig
	var ok bool

	c.path, ok = params["path"].(string)
	if !ok {
		return nil, fmt.Errorf("'path' field is missing or not string")
	}

	return &c, nil
}

func (c *badgerdsDatastoreConfig) DiskId() string {
	return fmt.Sprintf("badgerds;%s", c.path)
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

	return badgerds.NewDatastore(p, nil)
}
