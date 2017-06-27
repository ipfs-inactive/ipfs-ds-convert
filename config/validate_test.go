package config

import "testing"

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
	err := Validate(TestSpec)
	if err != nil {
		t.Errorf("Should not return error: %s", err)
	}

	//TODO: better coverage
}
