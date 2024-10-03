package config

import (
	"encoding/json"
	"os"
)

func Load(path string, out *map[string]interface{}) error {
	cfgbytes, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	err = json.Unmarshal(cfgbytes, out)
	if err != nil {
		return err
	}

	return nil
}
