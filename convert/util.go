package convert

import (
	"fmt"
	"strings"

	errors "github.com/pkg/errors"
)

func (c *Conversion) addStep(format string, args ...interface{}) {
	c.steps = append(c.steps, fmt.Sprintf(format, args...))
}

func (c *Conversion) wrapErr(err error) error {
	s := strings.Join(c.steps, "\n")

	return errors.Wrapf(err, "CONVERSION ERROR\n----------\nConversion steps done so far:\n%s\n----------\n", s)
}
