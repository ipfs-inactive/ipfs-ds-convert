package convert

import (
	"fmt"
	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"

	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

var ErrMountNotSimple = errors.New("mount is not simple")

var skipTypes = map[string]string{
	"measure": "child",
	"log":     "child",
}

var dsTypes = map[string]bool{
	"flatfs":   true,
	"levelds":  true,
	"badgerds": true,
}

//datastors that have one directory inside IPFS repo
var simpleTypes = map[string]bool{
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

		mountpoint, has := specIn.str("mountpoint")
		if has {
			child["mountpoint"] = mountpoint
		}

		return cleanUp(child)
	}

	_, isDs := dsTypes[t]
	if isDs {
		return specIn, nil
	}

	switch {
	case t == "mount":
		mounts, ok := specIn["mounts"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("'mounts' field is missing or not an array")
		}
		var outSpec = Spec{}
		outSpec["type"] = "mount"
		var outMounts []interface{}

		for _, m := range mounts {
			mount, ok := m.(Spec)
			if !ok {
				return nil, fmt.Errorf("'mounts' element is of invalid type")
			}

			cleanMount, err := cleanUp(mount)
			if err != nil {
				return nil, err
			}

			outMounts = append(outMounts, cleanMount)
		}

		outSpec["mounts"] = outMounts

		return outSpec, nil
	default:
		return nil, fmt.Errorf("unknown or unsupported type '%s' in datasotre spec", t)
	}
}

func simpleMountInfo(mountSpec Spec) (SimpleMounts, error) {
	mounts, ok := mountSpec["mounts"].([]interface{})
	if !ok {
		return nil, errors.New("'mounts' field is missing or not an array")
	}

	var simpleMounts []SimpleMount
	for _, m := range mounts {
		mount, ok := m.(Spec)
		if !ok {
			return nil, fmt.Errorf("'mounts' element is of invalid type")
		}

		dsType, ok := mount.dsType()
		if !ok {
			return nil, fmt.Errorf("mount type is not defined or of invalid type")
		}

		if _, ok := simpleTypes[dsType]; !ok {
			return nil, ErrMountNotSimple
		}

		prefix, ok := mount.str("mountpoint")
		if !ok {
			fmt.Println(mount)
			return nil, fmt.Errorf("mount field 'mountpoint' is not defined or of invalid type")
		}

		simpleMounts = append(simpleMounts, SimpleMount{prefix: ds.NewKey(prefix), diskId: DatastoreSpec(mount), spec: mount})
	}

	return simpleMounts, nil
}

//TODO: wire up to main process, cleanup
func NewStrategy(fromSpecIn, toSpecIn map[string]interface{}) (Strategy, error) {
	fromSpec, err := cleanUp(fromSpecIn)
	if err != nil {
		return nil, err
	}

	toSpec, err := cleanUp(toSpecIn)
	if err != nil {
		return nil, err
	}

	fromType, _ := fromSpec.dsType()
	toType, _ := toSpec.dsType()

	if _, ok := dsTypes[fromType]; ok {
		if toType == fromType {
			//TODO: check if dirs match, can just skip conversion, else just move directories
			return NewCopyStrategy(fromSpec, toSpec), nil
		}

		//TODO: might still be able to optimize if toType is single element mount
		return NewCopyStrategy(fromSpec, toSpec), nil
	}

	if fromType == "mount" {
		if toType != "mount" {
			//TODO: this can be possible to optimize in case there is only one element
			//in mount, but it's probably not worth it
			return NewCopyStrategy(fromSpec, toSpec), nil
		}

		//TODO create more test cases

		//TODO: see if duplicate mount prefix is filtered out
		var skipable []SimpleMount

		fMounts, err := simpleMountInfo(fromSpec)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing old spec")
		}

		tMounts, err := simpleMountInfo(toSpec)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing new spec")
		}

		for _, from := range fMounts {
			if tMounts.hasMatching(from) {
				skipable = append(skipable, from)
			}
		}

		//TODO: handle renames, somehow

		fMounts = fMounts.filter(skipable)
		tMounts = tMounts.filter(skipable)

		//TODO filter out nested keys, BOTH WAYS

		return NewCopyStrategy(fMounts.spec(), tMounts.spec()), nil
	}

	//should not normally happen
	return nil, errors.New("unable to create conversion strategy")
}
