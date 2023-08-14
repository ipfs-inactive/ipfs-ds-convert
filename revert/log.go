package revert

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

const (
	ConvertLog = "convertlog"

	ActionRemove = Action("rm")
	ActionMove   = Action("mv")
	ActionMkdir  = Action("mkdir")
	ActionDone   = Action("done")

	//For breaking things that can't be easily recovered from, say writing new spec
	ActionManual = Action("manual")

	//ActionManual marks backup files that can be cleaned up after conversion with --keep
	ActionCleanup = Action("cleanup")
)

type Action string

type ActionLogger struct {
	repo string
	file *os.File
}

// NewActionLogger creates revert action logger which logs actions needed to
// revert conversion steps
func NewActionLogger(repoPath string) (*ActionLogger, error) {
	if _, err := os.Stat(path.Join(repoPath, ConvertLog)); !os.IsNotExist(err) {
		return nil, fmt.Errorf("Log file %s already exists, you may want to run revert", path.Join(repoPath, ConvertLog))
	}

	f, err := os.Create(path.Join(repoPath, ConvertLog))
	if err != nil {
		return nil, err
	}

	return &ActionLogger{
		repo: repoPath,
		file: f,
	}, nil
}

func (a *ActionLogger) Log(action Action, params ...string) error {
	if a == nil {
		return nil
	}

	d, err := action.Line(params...)
	if err != nil {
		return err
	}

	n, err := a.file.Write(d)
	if err != nil {
		return err
	}

	if n != len(d) {
		return fmt.Errorf("failed to write steps, wrote %d, expected %d", n, len(d))
	}

	return a.file.Sync()
}

func (a *ActionLogger) Close() {
	a.file.Close()
}

func (a *ActionLogger) CloseFinal() error {
	a.file.Close()

	return os.Remove(path.Join(a.repo, ConvertLog))
}

func (a Action) Line(arg ...string) ([]byte, error) {
	b, err := json.Marshal(map[string]interface{}{
		"action": a,
		"arg":    arg,
	})

	if err != nil {
		return nil, err
	}

	return append(b, "\n"...), nil
}
