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
			return NewCopyStrategy(fromSpec, toSpec)
		}

		//TODO: might still be able to optimize if toType is single element mount
		return NewCopyStrategy(fromSpec, toSpec)
	}

	if fromType == "mount" {
		if toType != "mount" {
			//TODO: this can be possible to optimize in case there is only one element
			//in mount, but it's probably not worth it
			return NewCopyStrategy(fromSpec, toSpec)
		}

		//TODO create more test cases

		//TODO: see if duplicate mount prefix is filtered out
		var skipable []SimpleMount

		fromMounts, err := simpleMountInfo(fromSpec)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing old spec")
		}

		toMounts, err := simpleMountInfo(toSpec)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing new spec")
		}

		for _, from := range fromMounts {
			if toMounts.hasMatching(from) {
				skipable = append(skipable, from)
			}
		}

		//TODO: handle renames, somehow

		fromMountsOpt := fromMounts.filter(skipable)
		toMountsOpt := toMounts.filter(skipable)

		fromMountsOpt.sort()
		toMountsOpt.sort()

		for _, toMount := range toMountsOpt {
			if fromMounts.hasPrefixed(toMount) == -1 {
				var bestMatch SimpleMount
				bestMatched := -1
				toParts := toMount.prefix.List()

				for _, fromMount := range fromMounts {
					matched := matchStringsPrefix(toParts, fromMount.prefix.List())
					if matched > bestMatched {
						bestMatched = matched
						bestMatch = fromMount
					}
				}

				if bestMatched == -1 {
					return nil, fmt.Errorf("couldn't find best match for toMount %s", toMount.prefix.String())
				}

				if fromMountsOpt.hasPrefixed(bestMatch) == -1 {
					fromMountsOpt = append(fromMountsOpt, bestMatch)
				}
				if toMountsOpt.hasPrefixed(bestMatch) == -1 {
					ti := toMounts.hasPrefixed(bestMatch)
					if ti == -1 {
						//TODO: fallback to copyAll
						return nil, fmt.Errorf("couldn't find %s in toMounts, parent of %s", bestMatch.prefix.String(), toMount)
					}
					toMountsOpt = append(toMountsOpt, toMounts[ti])
				}
			}
		}

		//TODO: Deduplicate
		for _, fromMount := range fromMountsOpt {
			if toMounts.hasPrefixed(fromMount) == -1 {
				var bestMatch SimpleMount
				bestMatched := -1
				fromParts := fromMount.prefix.List()

				for _, toMount := range toMounts {
					matched := matchStringsPrefix(fromParts, toMount.prefix.List())
					if matched > bestMatched {
						bestMatched = matched
						bestMatch = toMount
					}
				}

				if bestMatched == -1 {
					return nil, fmt.Errorf("couldn't find best match for fromMount %s", fromMount.prefix.String())
				}

				if toMountsOpt.hasPrefixed(bestMatch) == -1 {
					toMountsOpt = append(toMountsOpt, bestMatch)
				}
				if fromMountsOpt.hasPrefixed(bestMatch) == -1 {
					ti := fromMounts.hasPrefixed(bestMatch)
					if ti == -1 {
						//TODO: fallback to copyAll
						return nil, fmt.Errorf("couldn't find %s in fromMounts, parent of %s", bestMatch.prefix.String(), fromMount)
					}
					fromMountsOpt = append(fromMountsOpt, fromMounts[ti])
				}
			}
		}

		if len(fromMountsOpt) == 0 {
			if len(toMountsOpt) != 0 {
				return nil, fmt.Errorf("strategy error: len(toMounts) != 0, please report")
			}

			return NewNoopStrategy()
		}
		if len(toMountsOpt) == 0 {
			return nil, fmt.Errorf("strategy error: len(toMounts) == 0, please report")
		}

		return NewCopyStrategy(fromMountsOpt.spec(), toMountsOpt.spec())
	}

	//should not normally happen
	return nil, errors.New("unable to create conversion strategy")
}

func matchStringsPrefix(pattern, to []string) int {
	if len(pattern) == 1 && pattern[0] == "" {
		pattern = []string{}
	}

	if len(to) == 1 && to[0] == "" {
		to = []string{}
	}

	if len(to) > len(pattern) {
		return -1
	}

	for i, part := range to {
		if part != pattern[i] {
			if i == 0 {
				return -1
			}
			return i
		}
	}

	return len(to)
}
