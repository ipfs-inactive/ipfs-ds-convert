package convert_test

import (
	"testing"

	"github.com/ipfs/ipfs-ds-convert/convert"
	"strings"
)

type testCase struct {
	baseSpec convert.Spec
	destSpec convert.Spec
	strategy string
	err      string
}

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

	testCases = []testCase{
		////////////////////
		// MAIN LOGIC CASES

		{
			//Only 'transparent' layers are changed, no action should be taken
			baseSpec: basicSpec,
			destSpec: convert.Spec{
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
			},
			strategy: `{"type":"noop"}`,
		},
		{
			//Removed 'transparent' layers, no action should be taken
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/blocks",
						"type":       "flatfs",
						"path":       "blocks",
						"sync":       true,
						"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
					},
					convert.Spec{
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
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/blocks",
						"type":       "badgerds",
						"path":       "blocks",
					},
					convert.Spec{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"type":"copy","from":{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","type":"flatfs"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/blocks","path":"blocks","type":"badgerds"}],"type":"mount"}}`,
		},
		{
			//adds /foo mount, needs to copy [/,/foo]
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/blocks",
						"type":       "flatfs",
						"path":       "blocks",
						"sync":       true,
						"shardFunc":  "/repo/flatfs/shard/v1/next-to-last/2",
					},
					convert.Spec{
						"mountpoint": "/foo",
						"type":       "badgerds",
						"path":       "foo",
					},
					convert.Spec{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"type":"copy","from":{"mounts":[{"mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/foo","path":"foo","type":"badgerds"},{"mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"}}`,
		},
		{
			//has single / mount, needs to copy [/,/blocks]
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint":  "/",
						"type":        "levelds",
						"path":        "levelDatastore",
						"compression": "none",
					},
				},
			},
			strategy: `{"type":"copy","from":{"mounts":[{"mountpoint":"/blocks","path":"blocks","shardFunc":"/repo/flatfs/shard/v1/next-to-last/2","type":"flatfs"},{"mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/","path":"levelDatastore","type":"levelds"}],"type":"mount"}}`,
		},
		{
			//skippable spec from testfiles
			baseSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/a",
						"type":       "badgerds",
						"path":       "dsa",
					},
					convert.Spec{
						"mountpoint": "/b",
						"type":       "badgerds",
						"path":       "dsb",
					},
					convert.Spec{
						"mountpoint": "/c",
						"type":       "badgerds",
						"path":       "dsc",
					},
					convert.Spec{
						"mountpoint": "/",
						"type":       "badgerds",
						"path":       "ds",
					},
				},
			},
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/a",
						"type":       "badgerds",
						"path":       "dsa",
					},
					convert.Spec{
						"mountpoint": "/b",
						"type":       "levelds",
						"path":       "dsb",
						"compression": "none",
					},
					convert.Spec{
						"mountpoint": "/",
						"type":       "badgerds",
						"path":       "ds",
					},
					convert.Spec{
						"mountpoint": "/d",
						"type":       "badgerds",
						"path":       "dsc",
					},
				},
			},
			strategy: `{"type":"copy","from":{"mounts":[{"mountpoint":"/c","path":"dsc","type":"badgerds"},{"mountpoint":"/b","path":"dsb","type":"badgerds"},{"mountpoint":"/","path":"ds","type":"badgerds"}],"type":"mount"},"to":{"mounts":[{"mountpoint":"/d","path":"dsc","type":"badgerds"},{"mountpoint":"/b","path":"dsb","type":"levelds"},{"mountpoint":"/","path":"ds","type":"badgerds"}],"type":"mount"}}`,
		},
		////////////////////
		//EDGE CASES

		{
			//no dest type
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"mounts": []interface{}{
					convert.Spec{
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
			destSpec: convert.Spec{
				"type":   "measure",
				"prefix": "flatfs.datastore",
			},
			err: "missing 'child' field in datastore spec",
		},
		{
			//invalid child in measure
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"type":   "measure",
				"prefix": "flatfs.datastore",
				"child":  "foo",
			},
			err: "invalid 'child' field type in datastore spec",
		},
		{
			//mountless mount
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"type": "mount",
			},
			err: "'mounts' field is missing or not an array",
		},
		{
			//invalid mount mounts type
			baseSpec: basicSpec,
			destSpec: convert.Spec{
				"type":   "mount",
				"mounts": "Foo",
			},
			err: "'mounts' field is missing or not an array",
		},
		{
			//invalid mount
			baseSpec: basicSpec,
			destSpec: convert.Spec{
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
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
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
			destSpec: convert.Spec{
				"type": "not a valid ds type",
			},
			err: "unknown or unsupported type 'not a valid ds type' in datasotre spec",
		},
		{
			//missing dest point
			baseSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/foo",
						"type":       "badgerds",
						"path":       "foo",
					},
					convert.Spec{
						"mountpoint":  "/bar",
						"type":        "levelds",
						"path":        "bar",
						"compression": "none",
					},
				},
			},
			destSpec: convert.Spec{
				"type": "mount",
				"mounts": []interface{}{
					convert.Spec{
						"mountpoint": "/foo",
						"type":       "badgerds",
						"path":       "foo",
					},
				},
			},
			err: "couldn't find best match for fromMount /bar",
		},
	}
)

func TestNewStrategy(t *testing.T) {
	for _, c := range testCases {
		strat, err := convert.NewStrategy(c.baseSpec, c.destSpec)
		assert(t, (err == nil && c.err == "") || (c.err != "" && strings.Contains(err.Error(), c.err)), err)
		if c.err == "" {
			assert(t, strat.Id() == c.strategy, strat.Id())
		}
	}
}

func TestStrategyReverse(t *testing.T) {
	for _, c := range testCases {
		_, err := convert.NewStrategy(c.destSpec, c.baseSpec)
		assert(t, err == nil || c.err != "", err)
	}
}

func assert(t *testing.T, cond bool, err interface{}) {
	if !cond {
		t.Fatalf("assertion failed: %s", err)
	}
}
