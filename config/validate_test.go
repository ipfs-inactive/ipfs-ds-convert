package config

import (
	"sort"
	"testing"
)

var (
	TestSpec = map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "flatfs.datastore",
				"child": map[string]interface{}{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      true,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			map[string]interface{}{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": map[string]interface{}{
					"type":        "levelds",
					"path":        "levelDatastore",
					"compression": "none",
				},
			},
			map[string]interface{}{
				"mountpoint": "/other",
				"type":       "measure",
				"prefix":     "badger.datastore",
				"child": map[string]interface{}{
					"type":        "badgerds",
					"path":        "badgerDatastore",
					"compression": "none",
				},
			},
		},
	}
)

func TestValidate(t *testing.T) {
	dirs, err := Validate(TestSpec)
	if err != nil {
		t.Errorf("Should not return error: %s", err)
	}

	sort.Strings(dirs)

	if dirs[0] != "badgerDatastore" {
		t.Errorf(`dirs[0] != "badgerDatastore" got %s `, dirs[0])
	}
	if dirs[1] != "blocks" {
		t.Errorf(`dirs[0] != "blocks" got %s `, dirs[1])
	}
	if dirs[2] != "levelDatastore" {
		t.Errorf(`dirs[0] != "levelDatastore" got %s `, dirs[2])
	}

	//TODO: better coverage
}
