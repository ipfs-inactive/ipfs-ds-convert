package strategy

import (
	"encoding/json"

	"github.com/ipfs/ipfs-ds-convert/config"

	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

type Strategy interface {
	Spec() Spec
	Id() string
}

type copyStrategy struct {
	fromSpec Spec
	toSpec   Spec
}

func validateCopySpec(spec Spec) error {
	t, ok := spec.Type()
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

	_, err := config.Validate(spec, false)
	return err
}

func NewCopyStrategy(fromSpec Spec, toSpec Spec) (Strategy, error) {
	if err := validateCopySpec(fromSpec); err != nil {
		return nil, errors.Wrapf(err, "validating old copy spec")
	}
	if err := validateCopySpec(toSpec); err != nil {
		return nil, errors.Wrapf(err, "validating new copy spec")
	}

	return &copyStrategy{
		fromSpec: fromSpec,
		toSpec:   toSpec,
	}, nil
}

func (s *copyStrategy) Spec() Spec {
	return Spec{
		"type": "copy",
		"from": s.fromSpec,
		"to":   s.toSpec,
	}
}

func (s *copyStrategy) Id() string {
	b, _ := json.Marshal(s.Spec())
	return string(b)
}

type noopStrategy struct {
}

func NewNoopStrategy() (Strategy, error) {
	return &noopStrategy{}, nil
}

func (s *noopStrategy) Spec() Spec {
	return Spec{
		"type": "noop",
	}
}

func (s *noopStrategy) Id() string {
	b, _ := json.Marshal(s.Spec())
	return string(b)
}
