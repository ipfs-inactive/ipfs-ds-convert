package strategy_test

import (
	"strings"
	"testing"

	"github.com/ipfs/ipfs-ds-convert/strategy"
)

type testCase struct {
	baseSpec map[string]interface{}
	destSpec map[string]interface{}
	strategy string
	err      string
}

var (
	basicSpec = map[string]interface{}{
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
		},
	}

	testCases = []testCase{
		////////////////////
		// MAIN LOGIC CASES

		{
			//Only 'transparent' layers are changed, no action should be taken
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/blocks",
						"type":       "log",
						"name":       "flatfs",
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
						"prefix":     "otherprefix.datastore",
						"child": map[string]interface{}{
							"type":        "levelds",
							"path":        "levelDatastore",
							"compression": "none",
						},
					},
				},
			},
			strategy: `{"type":"noop"}`,
		},
		{
			//Removed 'transparent' layers, no action should be taken
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/blocks",
						"type":       "flatfs",
						"path":       "blocks",
						"sync":       true,
						"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
					},
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"type":"noop"}`,
		},
		{
			//changed /blocks, rest untouched
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/blocks",
						"type":       "badgerds",
						"path":       "blocks",
					},
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","sync":true,"type":"flatfs"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/blocks","path":"blocks","type":"badgerds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//changed /blocks, rest untouched
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/blocks",
						"type":       "badger2ds",
						"path":       "blocks",
					},
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","sync":true,"type":"flatfs"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/blocks","path":"blocks","type":"badger2ds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//adds /foo mount, needs to copy [/,/foo]
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/blocks",
						"type":       "flatfs",
						"path":       "blocks",
						"sync":       true,
						"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
					},
					map[string]interface{}{
						"mountpoint": "/foo",
						"type":       "badgerds",
						"path":       "foo",
					},
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"compression":"none","mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/foo","path":"foo","type":"badgerds"},{"compression":"none","mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//adds /foo mount, needs to copy [/,/foo]
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/blocks",
						"type":       "flatfs",
						"path":       "blocks",
						"sync":       true,
						"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
					},
					map[string]interface{}{
						"mountpoint": "/foo",
						"type":       "badger2ds",
						"path":       "foo",
					},
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"compression":"none","mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/foo","path":"foo","type":"badger2ds"},{"compression":"none","mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//has single / mount, needs to copy [/,/blocks]
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","sync":true,"type":"flatfs"},{"compression":"none","mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"to":{"mounts":[{"compression":"none","mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//skippable spec from testfiles
			baseSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badgerds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint": "/b",
						"type":       "badgerds",
						"path":       "dsb",
					},
					map[string]interface{}{
						"mountpoint": "/c",
						"type":       "badgerds",
						"path":       "dsc",
					},
					map[string]interface{}{
						"mountpoint": "/",
						"type":       "badgerds",
						"path":       "ds",
					},
				},
			},
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badgerds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint":  "/b",
						"type":        "levelds",
						"path":        "dsb",
						"compression": "none",
					},
					map[string]interface{}{
						"mountpoint": "/",
						"type":       "badgerds",
						"path":       "ds",
					},
					map[string]interface{}{
						"mountpoint": "/d",
						"type":       "badgerds",
						"path":       "dsc",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"mountpoint":"/c","path":"dsc","type":"badgerds"},{"mountpoint":"/b","path":"dsb","type":"badgerds"},{"mountpoint":"/","path":"ds","type":"badgerds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/d","path":"dsc","type":"badgerds"},{"compression":"none","mountpoint":"/b","path":"dsb","type":"levelds"},{"mountpoint":"/","path":"ds","type":"badgerds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//skippable spec from testfiles
			baseSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badger2ds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint": "/b",
						"type":       "badger2ds",
						"path":       "dsb",
					},
					map[string]interface{}{
						"mountpoint": "/c",
						"type":       "badger2ds",
						"path":       "dsc",
					},
					map[string]interface{}{
						"mountpoint": "/",
						"type":       "badger2ds",
						"path":       "ds",
					},
				},
			},
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badger2ds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint":  "/b",
						"type":        "levelds",
						"path":        "dsb",
						"compression": "none",
					},
					map[string]interface{}{
						"mountpoint": "/",
						"type":       "badger2ds",
						"path":       "ds",
					},
					map[string]interface{}{
						"mountpoint": "/d",
						"type":       "badger2ds",
						"path":       "dsc",
					},
				},
			},
			strategy: `{"from":{"mounts":[{"mountpoint":"/c","path":"dsc","type":"badger2ds"},{"mountpoint":"/b","path":"dsb","type":"badger2ds"},{"mountpoint":"/","path":"ds","type":"badger2ds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/d","path":"dsc","type":"badger2ds"},{"compression":"none","mountpoint":"/b","path":"dsb","type":"levelds"},{"mountpoint":"/","path":"ds","type":"badger2ds"}],"type":"mount"},"type":"copy"}`,
		},
		{
			//from nested mount
			baseSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badgerds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint": "/b",
						"type":       "badgerds",
						"path":       "dsb",
					},
					map[string]interface{}{
						"type":       "mount",
						"mountpoint": "/c",
						"mounts": []interface{}{
							map[string]interface{}{
								"mountpoint": "/a",
								"type":       "badgerds",
								"path":       "dsc",
							},
							map[string]interface{}{
								"mountpoint": "/",
								"type":       "badgerds",
								"path":       "ds",
							},
						},
					},
				},
			},
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badgerds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint":  "/b",
						"type":        "levelds",
						"path":        "dsb",
						"compression": "none",
					},
					map[string]interface{}{
						"mountpoint": "/",
						"type":       "badgerds",
						"path":       "ds",
					},
					map[string]interface{}{
						"mountpoint": "/d",
						"type":       "badgerds",
						"path":       "dsc",
					},
				},
			},
			err: "parsing old spec: mount entry is not simple, mount datastores can't be nested",
		},
		{
			//from nested mount
			baseSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badger2ds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint": "/b",
						"type":       "badger2ds",
						"path":       "dsb",
					},
					map[string]interface{}{
						"type":       "mount",
						"mountpoint": "/c",
						"mounts": []interface{}{
							map[string]interface{}{
								"mountpoint": "/a",
								"type":       "badger2ds",
								"path":       "dsc",
							},
							map[string]interface{}{
								"mountpoint": "/",
								"type":       "badger2ds",
								"path":       "ds",
							},
						},
					},
				},
			},
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/a",
						"type":       "badger2ds",
						"path":       "dsa",
					},
					map[string]interface{}{
						"mountpoint":  "/b",
						"type":        "levelds",
						"path":        "dsb",
						"compression": "none",
					},
					map[string]interface{}{
						"mountpoint": "/",
						"type":       "badger2ds",
						"path":       "ds",
					},
					map[string]interface{}{
						"mountpoint": "/d",
						"type":       "badger2ds",
						"path":       "dsc",
					},
				},
			},
			err: "parsing old spec: mount entry is not simple, mount datastores can't be nested",
		},
		////////////////////
		//EDGE CASES

		{
			//no dest type
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			err: "invalid or missing 'type' in datastore spec",
		},
		{
			//childless measure
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type":   "measure",
				"prefix": "flatfs.datastore",
			},
			err: "missing 'child' field in datastore spec",
		},
		{
			//invalid child in measure
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type":   "measure",
				"prefix": "flatfs.datastore",
				"child":  "foo",
			},
			err: "invalid 'child' field type in datastore spec",
		},
		{
			//mountless mount
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
			},
			err: "'mounts' field is missing or not an array",
		},
		{
			//invalid mount mounts type
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type":   "mount",
				"mounts": "Foo",
			},
			err: "'mounts' field is missing or not an array",
		},
		{
			//invalid mount
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					"Foo",
				},
			},
			err: "'mounts' element is of invalid type",
		},
		{
			//invalid mount element
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"type":   "measure",
						"prefix": "flatfs.datastore",
					},
				},
			},
			err: "missing 'child' field in datastore spec",
		},
		{
			//invalid datastore
			baseSpec: basicSpec,
			destSpec: map[string]interface{}{
				"type": "not a valid ds type",
			},
			err: "unknown or unsupported type 'not a valid ds type' in datasotre spec",
		},
		{
			//missing dest point
			baseSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/foo",
						"type":       "badgerds",
						"path":       "foo",
					},
					map[string]interface{}{
						"mountpoint":  "/bar",
						"type":        "levelds",
						"path":        "bar",
						"compression": "none",
					},
				},
			},
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/foo",
						"type":       "badgerds",
						"path":       "foo",
					},
				},
			},
			err: "adding missing to src spec: couldn't find best match for specA /bar",
		},
		{
			//missing dest point
			baseSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/foo",
						"type":       "badger2ds",
						"path":       "foo",
					},
					map[string]interface{}{
						"mountpoint":  "/bar",
						"type":        "levelds",
						"path":        "bar",
						"compression": "none",
					},
				},
			},
			destSpec: map[string]interface{}{
				"type": "mount",
				"mounts": []interface{}{
					map[string]interface{}{
						"mountpoint": "/foo",
						"type":       "badger2ds",
						"path":       "foo",
					},
				},
			},
			err: "adding missing to src spec: couldn't find best match for specA /bar",
		},
	}
)

func TestNewStrategy(t *testing.T) {
	for _, c := range testCases {
		strat, err := strategy.NewStrategy(c.baseSpec, c.destSpec)
		assert(t, (err == nil && c.err == "") || (c.err != "" && strings.Contains(err.Error(), c.err)), err)
		if c.err == "" {
			assert(t, strat.Id() == c.strategy, strat.Id())
		}
	}
}

func TestStrategyReverse(t *testing.T) {
	for _, c := range testCases {
		_, err := strategy.NewStrategy(c.destSpec, c.baseSpec)
		assert(t, err == nil || c.err != "", err)
	}
}

func assert(t *testing.T, cond bool, err interface{}) {
	if !cond {
		t.Fatalf("assertion failed: %s", err)
	}
}
