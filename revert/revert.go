package revert

import (
	"fmt"
	"os"
	"path/filepath"

	logging "log"

	"github.com/ipfs/ipfs-ds-convert/repo"

	"encoding/json"

	lock "github.com/ipfs/go-fs-lock"
	"github.com/ipfs/ipfs-ds-convert/config"
	"github.com/pkg/errors"
)

var Log = logging.New(os.Stderr, "revert ", logging.LstdFlags)

type process struct {
	repo  string
	force bool

	steps Steps
}

func Revert(repoPath string, force bool, fixSpec bool, cleanupMode bool) (err error) {
	//TODO: validate repo dir

	p := process{
		repo:  repoPath,
		force: force,
	}

	unlock, err := lock.Lock(p.repo, repo.LockFile)
	if err != nil {
		return err
	}
	defer unlock.Close()

	p.steps, err = loadLog(p.repo)
	if err != nil {
		return err
	}

	if cleanupMode {
		Log.Println("Start cleanup")
	} else {
		Log.Println("Start revert")
	}

	n := 0
	for {
		step := p.steps.top()
		if step.action == "" {
			break
		}

		if !cleanupMode {
			err = p.executeStep(step)
		} else {
			err = p.executeCleanupStep(step, n)
		}

		if err != nil {
			return err
		}

		err = p.steps.pop(p.repo)
		if err != nil {
			return err
		}

		n++
	}

	p.steps.write(p.repo)

	if fixSpec {
		Log.Println("Save datastore_spec into config")

		err := fixConfig(p.repo)
		if err != nil {
			return err
		}
	}

	Log.Println("All tasks finished")
	return nil
}

func (p *process) executeStep(step Step) error {
	switch step.action {
	case ActionDone:
		if !p.force {
			return fmt.Errorf("last conversion was successful, run with --force to revert")
		}

	case ActionRemove:
		if len(step.arg) != 1 {
			return fmt.Errorf("revert remove: arg count %d != 1", len(step.arg))
		}
		Log.Printf("remove '%s'", step.arg[0])

		err := os.RemoveAll(step.arg[0])
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	case ActionMove:
		if len(step.arg) != 2 {
			return fmt.Errorf("revert move: arg count %d != 2", len(step.arg))
		}
		Log.Printf("move '%s' -> '%s': ", step.arg[0], step.arg[1])

		if _, err := os.Stat(step.arg[0]); os.IsNotExist(err) {
			return fmt.Errorf("revert move: source file '%s' didn't exist", step.arg[0])
		}

		if _, err := os.Stat(step.arg[1]); !os.IsNotExist(err) {
			return fmt.Errorf("revert move: destination file '%s' did exist", step.arg[1])
		}

		err := os.Rename(step.arg[0], step.arg[1])
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	case ActionMkdir:
		if len(step.arg) != 1 {
			return fmt.Errorf("revert mkdir: arg count %d != 1", len(step.arg))
		}
		Log.Printf("mkdir '%s': ", step.arg[0])

		if _, err := os.Stat(step.arg[0]); !os.IsNotExist(err) {
			return fmt.Errorf("revert mkdir: destination '%s' did exist", step.arg[0])
		}

		err := os.MkdirAll(step.arg[0], 0755)
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	case ActionCleanup:
	default:
		return fmt.Errorf("unknown revert step '%s'", step.action)
	}

	return nil
}

func (p *process) executeCleanupStep(step Step, n int) error {
	if n == 0 && step.action != ActionDone {
		return fmt.Errorf("cannot cleanup after failed conversion")
	}

	switch step.action {
	case ActionDone:
	case ActionRemove:
	case ActionMove:
	case ActionMkdir:

	case ActionCleanup:
		if len(step.arg) != 1 {
			return fmt.Errorf("cleanup arg count %d != 1", len(step.arg))
		}
		Log.Printf("cleanup '%s'", step.arg[0])

		err := os.RemoveAll(step.arg[0])
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	default:
		return fmt.Errorf("unknown cleanup step '%s'", step.action)
	}

	return nil
}

func fixConfig(repoPath string) error {
	spec := make(map[string]interface{})
	err := config.Load(filepath.Join(repoPath, repo.SpecsFile), &spec)
	if err != nil {
		return err
	}

	_, err = config.Validate(spec, true)
	if err != nil {
		return errors.Wrapf(err, "validating datastore_spec spec")
	}

	repoConfig := make(map[string]interface{})
	err = config.Load(filepath.Join(repoPath, repo.ConfigFile), &repoConfig)
	if err != nil {
		return err
	}

	confDatastore, ok := repoConfig["Datastore"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid Datastore field in config")
	}

	confDatastore["Spec"] = spec

	err = os.Rename(filepath.Join(repoPath, repo.ConfigFile), filepath.Join(repoPath, "config-old"))
	if err != nil {
		return err
	}

	confBytes, err := json.MarshalIndent(repoConfig, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath.Join(repoPath, repo.ConfigFile), []byte(confBytes), 0660)
	if err != nil {
		return err
	}

	//TODO: might try opening the datastore to soo if config works and revert to old
	//config.

	err = os.Remove(filepath.Join(repoPath, "config-old"))

	return err
}
