package strategy

import (
	"fmt"

	"github.com/ipfs/ipfs-ds-convert/repo"

	ds "github.com/ipfs/go-datastore"
	errors "github.com/pkg/errors"
)

var ErrMountNotSimple = errors.New("mount entry is not simple, mount datastores can't be nested")

var skipTypes = map[string]string{
	"measure": "child",
	"log":     "child",
}

var dsTypes = map[string]bool{
	"flatfs":   true,
	"levelds":  true,
	"badgerds": true,
}

// datastors that have one directory inside IPFS repo
var simpleTypes = map[string]bool{
	"flatfs":   true,
	"levelds":  true,
	"badgerds": true,
}

func NewStrategy(fromSpecIn, toSpecIn map[string]interface{}) (Strategy, error) {
	var fromSpec Spec
	var toSpec Spec

	fromSpec, err := cleanUp(fromSpecIn)
	if err != nil {
		return nil, err
	}

	toSpec, err = cleanUp(toSpecIn)
	if err != nil {
		return nil, err
	}

	fromType, _ := fromSpec.Type()
	toType, _ := toSpec.Type()

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

		return newMountStrategy(fromSpec, toSpec)
	}

	//should not normally happen
	return nil, errors.New("unable to create conversion strategy")
}

func cleanUp(specIn Spec) (map[string]interface{}, error) {
	t, ok := specIn.Type()
	if !ok {
		return nil, errors.New("invalid or missing 'type' in datastore spec")
	}

	childField, skip := skipTypes[t]
	if skip {
		ch, ok := specIn[childField]
		if !ok {
			return nil, fmt.Errorf("missing '%s' field in datastore spec", childField)
		}

		var child Spec
		child, ok = ch.(map[string]interface{})
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
			var mount Spec
			mount, ok = m.(map[string]interface{})
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
		var mount Spec
		mount, ok := m.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("'mounts' element is of invalid type")
		}

		dsType, ok := mount.Type()
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

		diskId, err := repo.DatastoreSpec(mount)
		if err != nil {
			return nil, err
		}

		simpleMounts = append(simpleMounts, SimpleMount{prefix: ds.NewKey(prefix), diskId: diskId, spec: mount})
	}

	return simpleMounts, nil
}

// addMissingParents adds missing roots to filtered specs
// spec A (source)
// /a
// /a/b
//
// spec B (dest)
// /a
//
// Assuming /a are matching, they are filtered out and so data from /a/b would
// be lost. This function adds missing mounts back to optimized spec.
// Returns fixed SpecAOpt, SpecBOpt
func addMissingParents(specA SimpleMounts, specB SimpleMounts, specAOpt SimpleMounts, specBOpt SimpleMounts) (SimpleMounts, SimpleMounts, error) {
	for _, mountA := range specA {
		if specB.hasPrefixed(mountA) == -1 {
			var bestMatch SimpleMount
			bestMatched := -1
			toParts := mountA.prefix.List()

			for _, mountB := range specB {
				matched := matchKeyPartsPrefix(toParts, mountB.prefix.List())
				if matched > bestMatched {
					bestMatched = matched
					bestMatch = mountB
				}
			}

			if bestMatched == -1 {
				return nil, nil, fmt.Errorf("couldn't find best match for specA %s", mountA.prefix.String())
			}

			if specBOpt.hasPrefixed(bestMatch) == -1 {
				specBOpt = append(specBOpt, bestMatch)
			}
			if specAOpt.hasPrefixed(bestMatch) == -1 {
				ti := specA.hasPrefixed(bestMatch)
				if ti == -1 {
					//TODO: fallback to copyAll
					return nil, nil, fmt.Errorf("couldn't find %s in specA, parent of %s", bestMatch.prefix.String(), mountA.prefix.String())
				}
				specAOpt = append(specAOpt, specA[ti])
			}
		}
	}
	return specAOpt, specBOpt, nil
}

func newMountStrategy(fromSpec, toSpec map[string]interface{}) (Strategy, error) {
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

	toMountsOpt, fromMountsOpt, err = addMissingParents(fromMounts, toMounts, fromMountsOpt, toMountsOpt)
	if err != nil {
		return nil, errors.Wrapf(err, "adding missing to src spec")
	}

	fromMountsOpt, toMountsOpt, err = addMissingParents(toMounts, fromMounts, toMountsOpt, fromMountsOpt)
	if err != nil {
		return nil, errors.Wrapf(err, "adding missing to dest spec")
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

func matchKeyPartsPrefix(pattern, to []string) int {
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
