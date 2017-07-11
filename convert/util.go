package convert

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	errors "gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

func LoadConfig(path string, out *map[string]interface{}) error {
	cfgbytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = json.Unmarshal(cfgbytes, out)
	if err != nil {
		return err
	}

	return nil
}

func (c *conversion) addStep(format string, args ...interface{}) {
	c.steps = append(c.steps, fmt.Sprintf(format, args...))
}

func (c *conversion) wrapErr(err error) error {
	s := strings.Join(c.steps, "\n")

	return errors.Wrapf(err, "CONVERSION ERROR\n----------\nConversion steps done so far:\n%s\n----------\n", s)
}
