package convert

import (
	"fmt"
	"github.com/pkg/errors"
)

const (
	StratCopyAll = "copyAll"
)

type Spec map[string]interface{}

func (s *Spec) dsType() (string, bool) {
	t, ok := (*s)["type"]
	if !ok {
		return "", false
	}
	ts, ok := t.(string)
	return ts, ok
}

var skipTypes = map[string]string{
	"measure": "child",
	"log":     "child",
}

var dsTypes = map[string]bool{
	"flatfs":   true,
	"levelds":  true,
	"badgerds": true,
}

func cleanUp(specIn Spec) (Spec, error) {
	t, ok := specIn.dsType()
	if !ok {
		return nil, errors.New("invalid or missing 'type' in datastore spec")
	}

	childField, skip := skipTypes[t]
	if skip {
		ch, ok := specIn[childField]
		if !ok {
			return nil, fmt.Errorf("missing '%s' field in datastore spec", childField)
		}

		child, ok := ch.(Spec)
		if !ok {
			return nil, fmt.Errorf("invalid '%s' field type in datastore spec", childField)
		}

		return cleanUp(child)
	}

	_, ds := dsTypes[t]
	if ds {
		return specIn, nil
	}

	switch {
	case t == "mount":
		mounts, ok := specIn["mounts"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("'mounts' field is missing or not an array")
		}
		var outSpec Spec
		outSpec["type"] = "mount"
		var outMounts []interface{}

		for _, m := range mounts {
			mount, ok := m.(Spec)
			if !ok {
				return nil, fmt.Errorf("'mounts' element is of invalid type")
			}

			outMounts = append(outMounts, cleanUp(mount))
		}

		outSpec["mounts"] = outMounts

		return outSpec, nil
	default:
		return nil, fmt.Errorf("unknown or unsupported type '%s' in datasotre spec", t)
	}
}

func NewStrategy(fromSpecIn, toSpecIn map[string]interface{}) (string, error) {
	fromSpec, err := cleanUp(fromSpecIn)
	if err != nil {
		return "", err
	}

	toSpec, err := cleanUp(toSpecIn)
	if err != nil {
		return "", err
	}

	fromType, _ := fromSpec.dsType()
	toType, _ := toSpec.dsType()

	if _, ok := dsTypes[fromType]; ok {
		if toType == fromType {
			//TODO: check if dirs match, can just skip conversion, else just move directories
			return StratCopyAll, nil
		}

		//TODO: might still be able to optimize if toType is single element mount
		return StratCopyAll, nil
	}

	if fromType == "mount" {
		if toType != "mount" {
			//TODO: this can be possible to optimize in case there is only one element
			//in mount, but it's probably not worth it
			return StratCopyAll, nil
		}

		//TODO: Do

		//create more test cases

		//filter out matching paths/datastore pairs

		//filter out nested keys, BOTH WAYS

		return StratCopyAll, nil
	}

	//should not normally happen
	return "", errors.New("unable to create conversion strategy")
}
