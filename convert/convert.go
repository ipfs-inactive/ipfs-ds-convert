package convert

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	logging "log"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
	errors "gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
	lock "gx/ipfs/QmWi28zbQG6B1xfaaWx5cYoLn3kBFU6pQ6GWQNRV5P6dNe/lock"
)

const (
	LockFile   = "repo.lock"
	ConfigFile = "config"
	SpecsFile  = "spec"

	SuppertedRepoVersion = 6
	ToolVersion          = "0.0.1"
)

var Log = logging.New(os.Stderr, "convert ", logging.LstdFlags)

type StageCtx struct {
	repoPath string
}

type ConversionStage interface {
	apply(*StageCtx) error
	unapply(*StageCtx) error
}

// conversion holds conversion state and progress
type conversion struct {
	steps []string

	path     string
	newDsDir string
	oldDsDir string //used after conversion

	dsSpec    map[string]interface{}
	newDsSpec map[string]interface{}

	oldPaths []string
	newPaths []string

	strategy []ConversionStage

	oldDs Datastore
	newDs Datastore
}

func Convert(repoPath string) error {
	c := conversion{
		path: repoPath,
	}

	c.addStep("begin with tool version %s", ToolVersion)

	err := c.checkRepoVersion()
	if err != nil {
		return err
	}

	unlock, err := lock.Lock(filepath.Join(c.path, LockFile))
	if err != nil {
		return err
	}
	defer unlock.Close()

	err = c.loadSpecs()
	if err != nil {
		return err
	}

	err = c.validateSpecs()
	if err != nil {
		return err
	}

	Log.Println("Checks OK")

	err = c.openDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	Log.Println("Copying keys, this can take a long time")

	err = c.copyAllKeys()
	if err != nil {
		return c.wrapErr(err)
	}

	err = c.closeDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	Log.Println("All data copied, swapping datastores")

	err = c.swapDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	err = c.openSwappedDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	Log.Println("Verifying key integrity")
	verified, err := c.verifyKeys()
	if err != nil {
		return c.wrapErr(err)
	}
	Log.Printf("%d keys OK\n", verified)

	err = c.closeDatastores()
	if err != nil {
		return c.wrapErr(err)
	}

	Log.Println("Saving new spec")
	err = c.saveNewSpec()
	if err != nil {
		return c.wrapErr(err)
	}

	//TODO: may want to check config even though there is probably little that can
	//go wrong unnoticed there

	Log.Println("All tasks finished")
	return nil
}

func (c *conversion) openDatastores() (err error) {
	c.oldDs, err = OpenDatastore(c.path, c.dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening datastore at %s", c.path)
	}
	c.addStep("open datastore at %s", c.path)

	c.newDsDir, err = ioutil.TempDir(c.path, "ds-convert")
	if err != nil {
		return errors.Wrapf(err, "error creating temp datastore at %s", c.path)
	}
	c.addStep("create temp datastore directory at %s", c.newDsDir)

	c.newDs, err = OpenDatastore(c.newDsDir, c.newDsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening new datastore at %s", c.newDsDir)
	}
	c.addStep("open new datastore at %s", c.newDsDir)

	return nil
}

func (c *conversion) closeDatastores() error {
	err := c.oldDs.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing old datastore")
	}
	c.addStep("close old datastore")

	err = c.newDs.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing new datastore")
	}
	c.addStep("close new datastore")
	return nil
}

func (c *conversion) copyAllKeys() error {
	c.addStep("start copying data")
	return CopyKeys(c.oldDs, c.newDs)
}

func CopyKeys(fromDs Datastore, toDs Datastore) error {
	//flatfs only supports KeysOnly:true
	//TODO: try to optimize this
	res, err := fromDs.Query(dsq.Query{Prefix: "/", KeysOnly: true})
	if err != nil {
		return errors.Wrapf(err, "error opening query")
	}
	defer res.Close()

	maxBatchEntries := 1024
	maxBatchSize := 16 << 20

	curEntries := 0
	curSize := 0

	var curBatch ds.Batch

	for {
		entry, ok := res.NextSync()
		if entry.Error != nil {
			return errors.Wrapf(entry.Error, "entry.Error was not nil")
		}
		if !ok {
			break
		}

		if curBatch == nil {
			curBatch, err = toDs.Batch()
			if entry.Error != nil {
				return errors.Wrapf(err, "error creating batch")
			}
			if curBatch == nil {
				return errors.New("failed to create new batch")
			}
		}

		val, err := fromDs.Get(ds.RawKey(entry.Key))
		if err != nil {
			return errors.New("get from old datastore failed")
		}

		curBatch.Put(ds.RawKey(entry.Key), val)
		curEntries++

		bval, ok := val.([]byte)
		if ok {
			curSize += len(bval)
		}

		if curEntries == maxBatchEntries || curSize >= maxBatchSize {
			err := curBatch.Commit()
			if err != nil {
				return errors.Wrapf(err, "batch commit failed")
			}

			curEntries = 0
			curSize = 0
		}

	}

	if curEntries > 0 {
		if curBatch == nil {
			return errors.New("nil curBatch when there are unflushed entries")
		}

		err := curBatch.Commit()
		if err != nil {
			return errors.Wrapf(err, "batch commit failed")
		}
	}
	return nil
}

func (c *conversion) swapDatastores() (err error) {
	c.oldDsDir, err = ioutil.TempDir(c.path, "ds-convert-old")
	if err != nil {
		return errors.Wrapf(err, "error creating temp datastore at %s", c.path)
	}
	c.addStep("create temp datastore directory at %s", c.oldDsDir)

	//TODO: Check if old dirs aren't mount points
	for _, dir := range c.oldPaths {
		err := os.Rename(path.Join(c.path, dir), path.Join(c.oldDsDir, dir))
		if err != nil {
			return errors.Wrapf(err, "error moving old datastore dir %s to %s", dir, c.oldDsDir)
		}
		c.addStep("> move %s to %s", path.Join(c.path, dir), path.Join(c.oldDsDir, dir))

		//Those are theoretically not needed, but having them won't hurt
		if _, err := os.Stat(path.Join(c.path, dir)); !os.IsNotExist(err) {
			return fmt.Errorf("failed to move old datastore dir %s from %s", dir, c.path)
		}

		if s, err := os.Stat(path.Join(c.oldDsDir, dir)); err != nil || !s.IsDir() {
			return fmt.Errorf("failed to move old datastore dir %s to %s", dir, c.oldDsDir)
		}
	}
	c.addStep("move old DS to %s", c.oldDsDir)

	for _, dir := range c.newPaths {
		err := os.Rename(path.Join(c.newDsDir, dir), path.Join(c.path, dir))
		if err != nil {
			return errors.Wrapf(err, "error moving new datastore dir %s from %s", dir, c.newDsDir)
		}
		c.addStep("> move %s to %s", path.Join(c.newDsDir, dir), path.Join(c.path, dir))
	}
	c.addStep("move new DS from %s", c.oldDsDir)

	//check if newDs dir is empty
	dir, err := os.Open(c.newDsDir)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", c.newDsDir)
	}

	_, err = dir.Readdirnames(1)
	if err != io.EOF {
		dir.Close()
		return fmt.Errorf("%s was not empty after swapping repos", c.newDsDir)
	}
	dir.Close()

	err = os.Remove(c.newDsDir)
	if err != nil {
		return fmt.Errorf("failed to remove newDs temp directory after swapping repos")
	}

	c.addStep("remove temp newDs directory %s", c.newDsDir)

	return nil
}

func (c *conversion) openSwappedDatastores() (err error) {
	c.oldDs, err = OpenDatastore(c.oldDsDir, c.dsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening datastore at %s", c.oldDsDir)
	}
	c.addStep("open datastore at %s", c.oldDsDir)

	c.newDs, err = OpenDatastore(c.path, c.newDsSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening new datastore at %s", c.path)
	}
	c.addStep("open new datastore at %s", c.path)

	return nil
}

func (c *conversion) verifyKeys() (n int, err error) {
	c.addStep("verify keys")

	res, err := c.oldDs.Query(dsq.Query{Prefix: "/", KeysOnly: true})
	if err != nil {
		return n, errors.Wrapf(err, "error opening query")
	}
	defer res.Close()

	for {
		entry, ok := res.NextSync()
		if entry.Error != nil {
			return n, errors.Wrapf(entry.Error, "entry.Error was not nil")
		}
		if !ok {
			break
		}

		has, err := c.newDs.Has(ds.RawKey(entry.Key))
		if err != nil {
			return n, errors.Wrapf(err, "newDs.Has returned error")
		}

		if !has {
			return n, fmt.Errorf("Key %s was nor present in new datastore", entry.Key)
		}

		n++
	}

	return n, nil
}

func (c *conversion) saveNewSpec() (err error) {
	specs := map[string]interface{}{
		"id":   DatastoreId(c.newDsSpec),
		"spec": c.newDsSpec,
	}

	specsPath := filepath.Join(c.path, SpecsFile)

	b, err := json.Marshal(specs)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(specsPath, b, 0660)
	if err != nil {
		return err
	}

	return nil
}
