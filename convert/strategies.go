package convert

import (
	"fmt"

	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
	"github.com/ipfs/ipfs-ds-convert/config"
)

type Strategy interface {
	Run() error
	Id() string
}

type copyStrategy struct {
	fromSpec Spec
	toSpec   Spec
}

func validateCopySpec(spec Spec) error {
	t, ok := spec.dsType()
	if !ok {
		return errors.New("copy spec has no type or field type is invalid")
	}

	if t == "mount" {
		mnts, ok := spec["mounts"]
		if !ok {
			return errors.New("copy spec has no mounts field")
		}

		mounts, ok := mnts.([]interface{})
		if !ok {
			return errors.New("copy spec has invalid mounts field type")
		}

		if len(mounts) == 0 {
			return errors.New("copy spec has empty mounts field")
		}
	}

	_, err := config.Validate(spec)
	return err
}

func NewCopyStrategy(fromSpec Spec, toSpec Spec) (Strategy, error) {
	if err := validateCopySpec(fromSpec); err != nil {
		return nil, err
	}
	if err := validateCopySpec(toSpec); err != nil {
		return nil, err
	}

	return &copyStrategy{
		fromSpec: fromSpec,
		toSpec:   toSpec,
	}, nil
}

func (s *copyStrategy) Run() error {
	return errors.New("TODO")
	//open, tospec as temp
	//return CopyKeys(s.fromDs, s.toDs)
}

func (s *copyStrategy) Id() string {
	return fmt.Sprintf(`{"type":"copy","from":%s,"to":%s}`, s.fromSpec.Id(), s.toSpec.Id())
}

type noopStrategy struct {
}

func NewNoopStrategy() (Strategy, error) {
	return &noopStrategy{}, nil
}

func (s *noopStrategy) Run() error {
	return nil
}

func (s *noopStrategy) Id() string {
	return fmt.Sprintf(`{"type":"noop"}`)
}
