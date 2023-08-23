package convert

import (
	"fmt"
	"os"
	"path/filepath"

	logging "log"

	"github.com/ipfs/ipfs-ds-convert/repo"
	"github.com/ipfs/ipfs-ds-convert/revert"
	"github.com/ipfs/ipfs-ds-convert/strategy"

	lock "github.com/ipfs/go-fs-lock"
)

var Log = logging.New(os.Stderr, "convert ", logging.LstdFlags)

// Conversion holds Conversion state and progress
type Conversion struct {
	steps []string
	log   *revert.ActionLogger

	path string

	fromSpec map[string]interface{}
	toSpec   map[string]interface{}
}

func Convert(repoPath string, keepBackup bool) error {
	c := Conversion{
		path: repoPath,
	}

	c.addStep("begin with tool version %s", repo.ToolVersion)

	err := c.checkRepoVersion()
	if err != nil {
		return err
	}

	unlock, err := lock.Lock(c.path, repo.LockFile)
	if err != nil {
		return err
	}
	defer unlock.Close()

	c.log, err = revert.NewActionLogger(c.path)
	if err != nil {
		return err
	}
	defer c.log.Close()

	err = c.loadSpecs()
	if err != nil {
		return err
	}

	s, err := strategy.NewStrategy(c.fromSpec, c.toSpec)
	if err != nil {
		return c.wrapErr(err)
	}

	strat := s.Spec()
	conversionType, _ := strat.Type()
	switch conversionType {
	case "copy":
		from, _ := strat.Sub("from")
		to, _ := strat.Sub("to")

		copy := NewCopy(c.path, from, to, c.log, c.addStep)
		err := copy.Run()
		if err != nil {
			return c.wrapErr(err)
		}

		err = copy.Verify()
		if err != nil {
			return c.wrapErr(err)
		}

		if !keepBackup {
			err = copy.Clean()
			if err != nil {
				return c.wrapErr(err)
			}
		}
	case "noop":
	default:
		panic(fmt.Sprintf("unexpected strategy %s", conversionType))
	}

	Log.Println("Saving new spec")
	err = c.saveNewSpec(keepBackup)
	if err != nil {
		return c.wrapErr(err)
	}

	c.log.Log(revert.ActionDone)

	if !keepBackup {
		err = c.log.CloseFinal()
		if err != nil {
			return err
		}
	}

	if keepBackup {
		Log.Println(">>            Backup files were not removed            <<")
		Log.Println(">> To revert to previous state run 'revert' subcommand <<")
		Log.Println(">>   To remove backup files run 'cleanup' subcommand   <<")
	}

	Log.Println("All tasks finished")
	return nil
}

func (c *Conversion) saveNewSpec(backup bool) (err error) {

	if backup {
		err = c.backupSpec()
		if err != nil {
			return err
		}
	} else {
		err = c.log.Log(revert.ActionManual, "restore datastore_spec to previous state")
		if err != nil {
			return err
		}
	}

	toDiskId, err := repo.DatastoreSpec(c.toSpec)
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(c.path, repo.SpecsFile), []byte(toDiskId), 0660)
	if err != nil {
		return err
	}

	if backup {
		err = c.log.Log(revert.ActionRemove, filepath.Join(c.path, repo.SpecsFile))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Conversion) backupSpec() error {
	backupFile, err := os.CreateTemp(c.path, "datastore_spec_backup")
	if err != nil {
		return err
	}

	specData, err := os.ReadFile(filepath.Join(c.path, repo.SpecsFile))
	if err != nil {
		return err
	}

	n, err := backupFile.Write(specData)
	if err != nil {
		return err
	}

	if n != len(specData) {
		return fmt.Errorf("failed to create backup of datastore_spec")
	}

	err = c.log.Log(revert.ActionMove, backupFile.Name(), filepath.Join(c.path, repo.SpecsFile))
	if err != nil {
		return err
	}

	err = c.log.Log(revert.ActionCleanup, backupFile.Name())
	if err != nil {
		return err
	}

	err = backupFile.Close()
	if err != nil {
		return err
	}

	return nil
}
