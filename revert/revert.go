package revert

import (
	"fmt"
	"os"
	"path/filepath"

	logging "log"

	"github.com/ipfs/ipfs-ds-convert/repo"

	lock "gx/ipfs/QmWi28zbQG6B1xfaaWx5cYoLn3kBFU6pQ6GWQNRV5P6dNe/lock"
)

var Log = logging.New(os.Stderr, "revert ", logging.LstdFlags)

type process struct {
	repo  string
	force bool

	steps Steps
}

func Revert(repoPath string, force bool) (err error) {
	//TODO: validate repo dir
	//TODO: cleanup mode after convert --keep
	//TODO: option to inject new spec to config

	p := process{
		repo:  repoPath,
		force: force,
	}

	unlock, err := lock.Lock(filepath.Join(p.repo, repo.LockFile))
	if err != nil {
		return err
	}
	defer unlock.Close()

	p.steps, err = loadLog(p.repo)
	if err != nil {
		return err
	}

	Log.Println("Start revert")

	for {
		step := p.steps.top()
		if step.action == "" {
			break
		}

		err := p.executeStep(step)
		if err != nil {
			return err
		}

		err = p.steps.pop(p.repo)
		if err != nil {
			return err
		}
	}

	p.steps.write(p.repo)

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
			return fmt.Errorf("revert remove arg count %d != 1", len(step.arg))
		}
		Log.Printf("remove '%s'", step.arg[0])

		err := os.RemoveAll(step.arg[0])
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	case ActionMove:
		if len(step.arg) != 2 {
			return fmt.Errorf("revert move arg count %d != 2", len(step.arg))
		}
		Log.Printf("move '%s' -> '%s': ", step.arg[0], step.arg[1])

		if _, err := os.Stat(step.arg[0]); os.IsNotExist(err) {
			return fmt.Errorf("revert move source file '%s' didn't exist", step.arg[0])
		}

		if _, err := os.Stat(step.arg[1]); !os.IsNotExist(err) {
			return fmt.Errorf("revert move destination file '%s' did exist", step.arg[1])
		}

		err := os.Rename(step.arg[0], step.arg[1])
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	case ActionMkdir:
		if len(step.arg) != 1 {
			return fmt.Errorf("revert mkdir arg count %d != 1", len(step.arg))
		}
		Log.Printf("mkdir '%s': ", step.arg[0])

		if _, err := os.Stat(step.arg[0]); !os.IsNotExist(err) {
			return fmt.Errorf("revert mkdir destination '%s' did exist", step.arg[0])
		}

		err := os.MkdirAll(step.arg[0], 0755)
		if err != nil {
			return err //TODO: wrap with more context?
		}

		Log.Println("\\-> ok")

	default:
		return fmt.Errorf("unknown revert step '%s'", step.action)
	}

	return nil
}
