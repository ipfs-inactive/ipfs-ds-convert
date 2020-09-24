package config

import (
	"sort"
	"strings"
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
			map[string]interface{}{
				"mountpoint": "/other2",
				"type":       "measure",
				"prefix":     "badger2.datastore",
				"child": map[string]interface{}{
					"type":        "badger2ds",
					"path":        "badger2Datastore",
					"compression": "none",
				},
			},
		},
	}

	EmptySpec = map[string]interface{}{}

	InvalidTypeSpec = map[string]interface{}{
		"type": 2,
	}

	InvalidFlatfsPathSpec = map[string]interface{}{
		"type":      "flatfs",
		"sync":      true,
		"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
	}

	InvalidBadgerdsPathSpec = map[string]interface{}{
		"type":        "badgerds",
		"compression": "none",
	}

	InvalidBadger2dsPathSpec = map[string]interface{}{
		"type":        "badger2ds",
		"compression": "none",
	}

	LeveldbNoCompression = map[string]interface{}{
		"type": "levelds",
		"path": "levelDatastore",
	}

	LeveldbNumericCompression = map[string]interface{}{
		"type":        "levelds",
		"path":        "levelDatastore",
		"compression": 2,
	}

	MountlessMount = map[string]interface{}{
		"type": "mount",
	}

	InvalidMount = map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			3,
		},
	}

	NoMountpoint = map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"type":        "levelds",
				"path":        "levelDatastore",
				"compression": 2,
			},
		},
	}

	DoubledMountpoint = map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"type":        "levelds",
				"path":        "levelDatastore1",
				"compression": "none",
				"mountpoint":  "/",
			},
			map[string]interface{}{
				"type":        "levelds",
				"path":        "levelDatastore2",
				"compression": "none",
				"mountpoint":  "/",
			},
		},
	}

	PrefixlessMeasure = map[string]interface{}{
		"mountpoint": "/blocks",
		"type":       "measure",
		"child": map[string]interface{}{
			"type":      "flatfs",
			"path":      "blocks",
			"sync":      true,
			"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
		},
	}

	ChildlessMeasure = map[string]interface{}{
		"mountpoint": "/blocks",
		"type":       "measure",
		"prefix":     "foo",
	}

	LogSpec = map[string]interface{}{
		"mountpoint": "/blocks",
		"type":       "log",
		"name":       "blocks",
		"child": map[string]interface{}{
			"type":      "flatfs",
			"path":      "blocks",
			"sync":      true,
			"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
		},
	}

	NamelessLogSpec = map[string]interface{}{
		"mountpoint": "/blocks",
		"type":       "log",
		"child": map[string]interface{}{
			"type":      "flatfs",
			"path":      "blocks",
			"sync":      true,
			"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
		},
	}

	ChildlessLogSpec = map[string]interface{}{
		"mountpoint": "/blocks",
		"type":       "log",
		"name":       "foo",
	}
)

func TestValidate(t *testing.T) {
	dirs, err := Validate(TestSpec, false)
	if err != nil {
		t.Errorf("should not return error: %s", err)
	}

	sort.Strings(dirs)

	if dirs[0] != "badger2Datastore" {
		t.Errorf(`dirs[0] != "badger2Datastore" got %s `, dirs[0])
	}
	if dirs[1] != "badgerDatastore" {
		t.Errorf(`dirs[0] != "badgerDatastore" got %s `, dirs[1])
	}
	if dirs[2] != "blocks" {
		t.Errorf(`dirs[0] != "blocks" got %s `, dirs[2])
	}
	if dirs[3] != "levelDatastore" {
		t.Errorf(`dirs[0] != "levelDatastore" got %s `, dirs[3])
	}
}

func TestEmptySpec(t *testing.T) {
	_, err := Validate(EmptySpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid type entry in config") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidTypeSpec(t *testing.T) {
	_, err := Validate(InvalidTypeSpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid type entry in config") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidFlatfsPathSpec(t *testing.T) {
	_, err := Validate(InvalidFlatfsPathSpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid 'path' type in datastore") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidBadgerdsPathSpec(t *testing.T) {
	_, err := Validate(InvalidBadgerdsPathSpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid 'path' type in datastore") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidBadger2dsPathSpec(t *testing.T) {
	_, err := Validate(InvalidBadger2dsPathSpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid 'path' type in datastore") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestLeveldbSpec(t *testing.T) {
	_, err := Validate(LeveldbNoCompression, false)
	if err != nil {
		if strings.Contains(err.Error(), "no compression field in leveldb spec") {
			_, err := Validate(LeveldbNoCompression, true)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			if LeveldbNoCompression["compression"] != "none" {
				t.Errorf("compression field not injected to leveldb spec")
			}
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestLeveldbNumSpec(t *testing.T) {
	_, err := Validate(LeveldbNumericCompression, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid compression field type in leveldb spec") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestMountlessMountSpec(t *testing.T) {
	_, err := Validate(MountlessMount, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid 'mounts' in mount datastore") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestInvalidMountSpec(t *testing.T) {
	_, err := Validate(InvalidMount, false)
	if err != nil {
		if strings.Contains(err.Error(), "mounts entry has invalid type") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestNoMountpointSpec(t *testing.T) {
	_, err := Validate(NoMountpoint, false)
	if err != nil {
		if strings.Contains(err.Error(), "'mountpoint' must be a string") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestDoubledMountpointSpec(t *testing.T) {
	_, err := Validate(DoubledMountpoint, false)
	if err != nil {
		if strings.Contains(err.Error(), "multiple mounts under one path are not allowed") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestPrefixlessMeasureSpec(t *testing.T) {
	_, err := Validate(PrefixlessMeasure, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid 'prefix' in measure datastore") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestChildlessMeasureSpec(t *testing.T) {
	_, err := Validate(ChildlessMeasure, false)
	if err != nil {
		if strings.Contains(err.Error(), "child of measure datastore has invalid type") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestLogSpec(t *testing.T) {
	_, err := Validate(LogSpec, false)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestNamelessLogSpec(t *testing.T) {
	_, err := Validate(NamelessLogSpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "invalid 'name' in log datastore") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}

func TestChildlessLogSpec(t *testing.T) {
	_, err := Validate(ChildlessLogSpec, false)
	if err != nil {
		if strings.Contains(err.Error(), "child of log datastore has invalid type") {
			return
		}
		t.Errorf("unexpected error: %s", err)
	}

	t.Errorf("expected error")
}
