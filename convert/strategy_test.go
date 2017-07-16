package convert_test

import (
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
)

var (
	basicSpec = convert.Spec{
		"type": "mount",
		"mounts": []interface{}{
			convert.Spec{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "flatfs.datastore",
				"child": convert.Spec{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      true,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			convert.Spec{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": convert.Spec{
					"type":        "levelds",
					"path":        "levelDatastore",
					"compression": "none",
				},
			},
		},
	}

	//Only 'transparent' layers are changed, no action should be taken
	matchingSpec = convert.Spec{
		"type": "mount",
		"mounts": []interface{}{
			convert.Spec{
				"mountpoint": "/blocks",
				"type":       "log",
				"name":       "flatfs",
				"child": convert.Spec{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      true,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			convert.Spec{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "otherprefix.datastore",
				"child": convert.Spec{
					"type":        "levelds",
					"path":        "levelDatastore",
					"compression": "none",
				},
			},
		},
	}

	//Removed 'transparent' layers, no action should be taken
	cleanSpec = convert.Spec{
		"type": "mount",
		"mounts": []interface{}{
			convert.Spec{
				"mountpoint": "/blocks",
				"type":      "flatfs",
				"path":      "blocks",
				"sync":      true,
				"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
			},
			convert.Spec{
				"mountpoint": "/",
				"type":        "levelds",
				"path":        "levelDatastore",
				"compression": "none",
			},
		},
	}

	//changed /blocks, rest untouched
	changeBlocksSpec = convert.Spec{
		"type": "mount",
		"mounts": []interface{}{
			convert.Spec{
				"mountpoint": "/blocks",
				"type":      "badgerds",
				"path":      "blocks",
			},
			convert.Spec{
				"mountpoint": "/",
				"type":        "levelds",
				"path":        "levelDatastore",
				"compression": "none",
			},
		},
	}
)

func TestNewStrategy(t *testing.T) {
	strat, err := convert.NewStrategy(basicSpec, matchingSpec)
	assert(t, err == nil, err)
	assert(t, strat.Id() == `{"type":"copy","from":{"mounts":[],"type":"mount"},"to":{"mounts":[],"type":"mount"}}`, strat.Id())

	strat, err = convert.NewStrategy(basicSpec, cleanSpec)
	assert(t, err == nil, err)
	assert(t, strat.Id() == `{"type":"copy","from":{"mounts":[],"type":"mount"},"to":{"mounts":[],"type":"mount"}}`, strat.Id())

	strat, err = convert.NewStrategy(basicSpec, changeBlocksSpec)
	assert(t, err == nil, err)
	assert(t, strat.Id() == `{"type":"copy","from":{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","type":"flatfs"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/blocks","path":"blocks","type":"badgerds"}],"type":"mount"}}`, strat.Id())
}

func assert(t *testing.T, cond bool, err interface{}) {
	if !cond {
		t.Fatalf("assertion failed: %s", err)
	}
}
