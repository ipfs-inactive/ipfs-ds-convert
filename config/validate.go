package config

import (
	"errors"
	"fmt"
	"path/filepath"
)

var (
	ErrInvalidType = errors.New("invalid type entry in config")
)

type validatorContext struct {
	usedPaths map[string]bool
}

var validators = map[string]func(*validatorContext, map[string]interface{}) error{}

func init() {
	validators["badgerds"] = badgerdsValidator
	validators["flatfs"] = flatfsValidator
	validators["levelds"] = leveldsValidator
	validators["log"] = logValidator
	validators["measure"] = measureValidator
	validators["mount"] = mountValidator
}

func Validate(dsConfiguration map[string]interface{}) (dirs []string, err error) {
	ctx := validatorContext{
		usedPaths: map[string]bool{},
	}
	err = validate(&ctx, dsConfiguration)

	paths := make([]string, 0, len(ctx.usedPaths))
	for k := range ctx.usedPaths {
		paths = append(paths, k)
	}

	return paths, err
}

func validate(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	t, ok := dsConfiguration["type"].(string)
	if !ok {
		return ErrInvalidType
	}

	validator := validators[t]
	if validator == nil {
		return fmt.Errorf("unsupported type entry in config: %s", t)
	}

	return validator(ctx, dsConfiguration)
}

func checkPath(ctx *validatorContext, p interface{}) error {
	path, ok := p.(string)
	if !ok {
		return errors.New("invalid 'path' type in flatfs datastore")
	}

	clean := filepath.Clean(path)
	if clean[0] == '/' || clean[0] == '.' {
		return errors.New("only paths inside ipfs repo are supported")
	}

	if ctx.usedPaths[path] {
		return fmt.Errorf("path '%s' is already in use", path)
	}

	ctx.usedPaths[path] = true

	//TODO: better path validation

	return nil
}

//////////////

func flatfsValidator(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	err := checkPath(ctx, dsConfiguration["path"])
	if err != nil {
		return err
	}

	return nil
}

func leveldsValidator(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	err := checkPath(ctx, dsConfiguration["path"])
	if err != nil {
		return err
	}

	//TODO: remove this horrible hack, and inject defaults properly into datastore_spec
	_, ok := dsConfiguration["compression"].(string)
	if !ok {
		//return errors.New("invalid 'compression' type in levelds datastore")
		dsConfiguration["compression"] = "none"
	}

	return nil
}

func badgerdsValidator(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	err := checkPath(ctx, dsConfiguration["path"])
	if err != nil {
		return err
	}

	return nil
}

func mountValidator(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	mounts, ok := dsConfiguration["mounts"].([]interface{})
	if !ok {
		return errors.New("invalid 'mounts' in mount datastore")
	}

	mountPoints := map[string]bool{}

	for _, m := range mounts {
		mount, ok := m.(map[string]interface{})
		if !ok {
			return errors.New("mounts entry has invalid type")
		}

		mountPoint, ok := mount["mountpoint"].(string)
		if !ok {
			return errors.New("'mountpoint' must be a string")
		}

		if mountPoints[mountPoint] {
			return errors.New("multiple mounts under one path are not allowed")
		}

		mountPoints[mountPoint] = true

		err := validate(ctx, mount)
		if err != nil {
			return err
		}
	}

	return nil
}

func measureValidator(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	_, ok := dsConfiguration["prefix"].(string)
	if !ok {
		return errors.New("invalid 'prefix' in measure datastore")
	}

	child, ok := dsConfiguration["child"].(map[string]interface{})
	if !ok {
		return errors.New("child of measure datastore has invalid type")
	}

	return validate(ctx, child)
}

func logValidator(ctx *validatorContext, dsConfiguration map[string]interface{}) error {
	_, ok := dsConfiguration["name"].(string)
	if !ok {
		return errors.New("invalid 'name' in log datastore")
	}

	child, ok := dsConfiguration["child"].(map[string]interface{})
	if !ok {
		return errors.New("child of log datastore has invalid type")
	}

	return validate(ctx, child)
}
