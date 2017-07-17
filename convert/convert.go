package convert

import (
	"io/ioutil"
	"os"
	"path/filepath"

	logging "log"

	"github.com/ipfs/ipfs-ds-convert/strategy"
	"github.com/ipfs/ipfs-ds-convert/repo"

	lock "gx/ipfs/QmWi28zbQG6B1xfaaWx5cYoLn3kBFU6pQ6GWQNRV5P6dNe/lock"
)

const (
	LockFile   = "repo.lock"
	ConfigFile = "config"
	SpecsFile  = "datastore_spec"

	SuppertedRepoVersion = 6
	ToolVersion          = "0.0.1"
)

var Log = logging.New(os.Stderr, "convert ", logging.LstdFlags)

// conversion holds conversion state and progress
type conversion struct {
	steps []string

	path string

	dsSpec    map[string]interface{}
	newDsSpec map[string]interface{}
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

	s, err := strategy.NewStrategy(c.dsSpec, c.newDsSpec)
	if err != nil {
		return c.wrapErr(err)
	}

	strat := s.Spec()
	conversionType, _ := strat.Type()
	switch conversionType {
	case "copy":
		from, _ := strat.Sub("from")
		to, _ := strat.Sub("to")

		copy := NewCopy(c.path, from, to, c.addStep)
		err := copy.Run()
		if err != nil {
			return c.wrapErr(err)
		}

		err = copy.Verify()
		if err != nil {
			return c.wrapErr(err)
		}
	case "noop":
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

func (c *conversion) saveNewSpec() (err error) {

	specsPath := filepath.Join(c.path, SpecsFile)

	err = ioutil.WriteFile(specsPath, []byte(repo.DatastoreSpec(c.newDsSpec)), 0660)
	if err != nil {
		return err
	}

	return nil
}
