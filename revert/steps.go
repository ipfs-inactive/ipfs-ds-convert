package revert

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
)

type Step struct {
	action Action
	arg    []string
}

type Steps []Step

func loadLog(repo string) (Steps, error) {
	var steps []Step

	f, err := os.Open(path.Join(repo, ConvertLog))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	log := bufio.NewReader(f)
	var readErr error
	for {
		var line string
		line, readErr = log.ReadString('\n')

		if line == "" {
			break
		}

		stepJson := map[string]interface{}{}
		err := json.Unmarshal([]byte(line), &stepJson)
		if err != nil {
			return nil, err
		}

		action, ok := stepJson["action"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid action type in convert steps: %s", line)
		}

		rawArgs, ok := stepJson["arg"].([]interface{})
		var args []string
		if ok {
			args = make([]string, 0, len(rawArgs))
			for i := range rawArgs {
				arg, ok := rawArgs[i].(string)
				if !ok {
					return nil, fmt.Errorf("invalid arg %d in convert steps: %s", i, line)
				}
				args = append(args, arg)
			}
		}

		steps = append(steps, Step{
			action: Action(action),
			arg:    args,
		})

		if readErr != nil {
			break
		}
	}

	if readErr != io.EOF {
		return nil, readErr
	}

	return steps, nil
}

func (s *Steps) top() Step {
	if len(*s) == 0 {
		return Step{}
	}
	return (*s)[len(*s) - 1]
}

func (s *Steps) pop(repo string) error {
	if len(*s) == 0 {
		return nil
	}
	*s = (*s)[:len(*s) - 1]

	return s.write(repo)
}

func (s *Steps) write(repo string) error {
	if len(*s) == 0 {
		return os.Remove(path.Join(repo, ConvertLog))
	}

	f, err := os.Create(path.Join(repo, ConvertLog))
	if err != nil {
		return  err
	}
	defer f.Close()

	for _, step := range *s {
		d, err := step.action.Line(step.arg...)
		if err != nil {
			return err
		}

		n, err := f.Write(d)
		if err != nil {
			return err
		}

		if n != len(d) {
			return fmt.Errorf("failed to write steps, wrote %d, expected %d", n, len(d))
		}
	}

	return nil
}
