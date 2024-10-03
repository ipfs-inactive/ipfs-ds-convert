package convert

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/ipfs/ipfs-ds-convert/config"
	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/revert"
	"github.com/ipfs/ipfs-ds-convert/strategy"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	errors "github.com/pkg/errors"
)

type Copy struct {
	path string

	fromSpec strategy.Spec
	toSpec   strategy.Spec

	newDsDir string
	oldDsDir string //used after conversion

	oldPaths []string
	newPaths []string

	fromDs repo.Datastore
	toDs   repo.Datastore

	log     *revert.ActionLogger
	logStep func(string, ...interface{})
}

func NewCopy(path string, fromSpec strategy.Spec, toSpec strategy.Spec, log *revert.ActionLogger, logStep func(string, ...interface{})) *Copy {
	return &Copy{
		path:     path,
		fromSpec: fromSpec,
		toSpec:   toSpec,
		log:      log,
		logStep:  logStep,
	}
}

func (c *Copy) Run() error {
	err := c.validateSpecs()
	if err != nil {
		return err
	}

	Log.Println("Checks OK")

	err = c.openDatastores()
	if err != nil {
		return err
	}

	Log.Println("Copying keys, this can take a long time")

	err = CopyKeys(c.fromDs, c.toDs)
	if err != nil {
		return err
	}

	err = c.closeDatastores()
	if err != nil {
		return err
	}

	Log.Println("All data copied, swapping repo")

	err = c.swapDatastores()
	if err != nil {
		return err
	}

	return nil
}

func (c *Copy) Verify() error {
	err := c.openSwappedDatastores()
	if err != nil {
		return err
	}

	Log.Println("Verifying key integrity")
	verified, err := c.verifyKeys()
	if err != nil {
		err2 := c.closeDatastores()
		if err2 != nil {
			return err2
		}

		return err
	}
	Log.Printf("%d keys OK\n", verified)

	err = c.closeDatastores()
	if err != nil {
		return err
	}

	return nil
}

func (c *Copy) validateSpecs() error {
	oldPaths, err := config.Validate(c.fromSpec, false)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", filepath.Join(c.path, repo.SpecsFile))
	}
	c.oldPaths = oldPaths

	newPaths, err := config.Validate(c.toSpec, false)
	if err != nil {
		return errors.Wrapf(err, "error validating datastore spec in %s", filepath.Join(c.path, repo.ConfigFile))
	}
	c.newPaths = newPaths

	return nil
}

func (c *Copy) openDatastores() (err error) {
	c.fromDs, err = repo.OpenDatastore(c.path, c.fromSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening datastore at %s", c.path)
	}
	c.logStep("open datastore at %s", c.path)

	c.newDsDir, err = os.MkdirTemp(c.path, "ds-convert")
	if err != nil {
		return errors.Wrapf(err, "error creating temp datastore at %s", c.path)
	}

	err = c.log.Log(revert.ActionRemove, c.newDsDir)
	if err != nil {
		return err
	}

	c.logStep("create temp datastore directory at %s", c.newDsDir)

	c.toDs, err = repo.OpenDatastore(c.newDsDir, c.toSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening new datastore at %s", c.newDsDir)
	}
	c.logStep("open new datastore at %s", c.newDsDir)

	return nil
}

func CopyKeys(fromDs repo.Datastore, toDs repo.Datastore) error {
	//flatfs only supports KeysOnly:true
	//TODO: try to optimize this
	res, err := fromDs.Query(dsq.Query{Prefix: "/", KeysOnly: true})
	if err != nil {
		return errors.Wrapf(err, "error opening query")
	}
	defer res.Close()

	maxBatchEntries := 1024
	maxBatchSize := 16 << 20

	doneEntries := 0
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
			return errors.Wrapf(err, "get from old datastore failed (dskey %s)", entry.Key)
		}

		curBatch.Put(ds.RawKey(entry.Key), val)
		curEntries++

		curSize += len(val)

		if curEntries == maxBatchEntries || curSize >= maxBatchSize {
			err := curBatch.Commit()
			if err != nil {
				return errors.Wrapf(err, "batch commit failed")
			}

			doneEntries += curEntries
			fmt.Printf("\rcopied %d keys", doneEntries)

			curEntries = 0
			curSize = 0
			curBatch = nil
		}
	}

	fmt.Printf("\rcopied %d keys", doneEntries+curEntries)
	fmt.Printf("\n")

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

func (c *Copy) swapDatastores() (err error) {
	c.oldDsDir, err = os.MkdirTemp(c.path, "ds-convert-old")
	if err != nil {
		return errors.Wrapf(err, "error creating temp datastore at %s", c.path)
	}

	err = c.log.Log(revert.ActionRemove, c.oldDsDir)
	if err != nil {
		return err
	}

	err = c.log.Log(revert.ActionCleanup, c.oldDsDir)
	if err != nil {
		return err
	}

	c.logStep("create temp datastore directory at %s", c.oldDsDir)

	//TODO: Check if old dirs aren't mount points
	for _, dir := range c.oldPaths {
		err := os.Rename(path.Join(c.path, dir), path.Join(c.oldDsDir, dir))
		if err != nil {
			return errors.Wrapf(err, "error moving old datastore dir %s to %s", dir, c.oldDsDir)
		}

		err = c.log.Log(revert.ActionMove, path.Join(c.oldDsDir, dir), path.Join(c.path, dir))
		if err != nil {
			return err
		}

		c.logStep("> move %s to %s", path.Join(c.path, dir), path.Join(c.oldDsDir, dir))

		//Those are theoretically not needed, but having them won't hurt
		if _, err := os.Stat(path.Join(c.path, dir)); !os.IsNotExist(err) {
			return fmt.Errorf("failed to move old datastore dir %s from %s", dir, c.path)
		}

		if s, err := os.Stat(path.Join(c.oldDsDir, dir)); err != nil || !s.IsDir() {
			return fmt.Errorf("failed to move old datastore dir %s to %s", dir, c.oldDsDir)
		}
	}
	c.logStep("move old DS to %s", c.oldDsDir)

	for _, dir := range c.newPaths {
		err := os.Rename(path.Join(c.newDsDir, dir), path.Join(c.path, dir))
		if err != nil {
			return errors.Wrapf(err, "error moving new datastore dir %s from %s", dir, c.newDsDir)
		}

		err = c.log.Log(revert.ActionMove, path.Join(c.path, dir), path.Join(c.newDsDir, dir))
		if err != nil {
			return err
		}

		c.logStep("> move %s to %s", path.Join(c.newDsDir, dir), path.Join(c.path, dir))
	}
	c.logStep("move new DS from %s", c.oldDsDir)

	//check if toDs dir is empty
	err = checkDirEmpty(c.newDsDir)
	if err != nil {
		return err
	}

	err = os.Remove(c.newDsDir)
	if err != nil {
		return fmt.Errorf("failed to remove toDs temp directory after swapping repos")
	}

	err = c.log.Log(revert.ActionMkdir, c.newDsDir)
	if err != nil {
		return err
	}

	c.logStep("remove temp toDs directory %s", c.newDsDir)

	return nil
}

func (c *Copy) openSwappedDatastores() (err error) {
	c.fromDs, err = repo.OpenDatastore(c.oldDsDir, c.fromSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening datastore at %s", c.oldDsDir)
	}
	c.logStep("open datastore at %s", c.oldDsDir)

	c.toDs, err = repo.OpenDatastore(c.path, c.toSpec)
	if err != nil {
		return errors.Wrapf(err, "error opening new datastore at %s", c.path)
	}
	c.logStep("open new datastore at %s", c.path)

	return nil
}

func (c *Copy) verifyKeys() (n int, err error) {
	c.logStep("verify keys")

	res, err := c.fromDs.Query(dsq.Query{Prefix: "/", KeysOnly: true})
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

		has, err := c.toDs.Has(ds.RawKey(entry.Key))
		if err != nil {
			return n, errors.Wrapf(err, "toDs.Has returned error")
		}

		if !has {
			return n, fmt.Errorf("key %s was not present in new datastore", entry.Key)
		}

		n++
	}

	return n, nil
}

func (c *Copy) closeDatastores() error {
	err := c.fromDs.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing old datastore")
	}
	c.logStep("close old datastore")

	err = c.toDs.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing new datastore")
	}
	c.logStep("close new datastore")
	return nil
}

func (c *Copy) Clean() error {
	err := c.log.Log(revert.ActionManual, "no backup data present for revert")
	if err != nil {
		return err
	}

	err = os.RemoveAll(c.oldDsDir)
	if err != nil {
		return fmt.Errorf("failed to remove oldDsDir temp directory")
	}

	return nil
}

func checkDirEmpty(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", path)
	}

	_, err = dir.Readdirnames(1)
	if err != io.EOF {
		dir.Close()
		return fmt.Errorf("%s is not empty", path)
	}
	return dir.Close()
}
