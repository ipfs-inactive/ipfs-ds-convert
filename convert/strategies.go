package convert

import (
	"fmt"

	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

type Strategy interface {
	Run() error
	Id() string
}

type copyStrategy struct {
	fromSpec Spec
	toSpec   Spec
}

func NewCopyStrategy(fromSpec Spec, toSpec Spec) Strategy {
	return &copyStrategy{
		fromSpec: fromSpec,
		toSpec:   toSpec,
	}
}

func (s *copyStrategy) Run() error {
	return errors.New("TODO")
	//open, tospec as temp
	//return CopyKeys(s.fromDs, s.toDs)
}

func (s *copyStrategy) Id() string {
	return fmt.Sprintf("copy:{%s};{%s}", s.fromSpec.Id(), s.toSpec.Id())
}
