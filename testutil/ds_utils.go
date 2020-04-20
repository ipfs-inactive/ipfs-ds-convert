package testutil

import (
	crand "crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"math/rand"

	repo "github.com/ipfs/go-ipfs/repo"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"

	"bytes"

	ds "github.com/ipfs/go-datastore"
)

// OpenRepo opens a repo.
func OpenRepo(repoPath string) (repo.Repo, error) {
	return fsrepo.Open(repoPath)
}

func getSeed() int64 {
	b := make([]byte, 8)
	crand.Read(b)
	return int64(binary.LittleEndian.Uint64(b))
}

// InsertRandomKeys puts random keys in a repo.
func InsertRandomKeys(prefix string, n int, r repo.Repo) (int64, error) {
	seed := getSeed()
	rnd := rand.New(rand.NewSource(seed))

	batch, err := r.Datastore().Batch()
	if err != nil {
		return 0, err
	}

	for i := 1; i <= n; i++ {
		keyBytes := make([]byte, 16)
		rnd.Read(keyBytes)
		dataBytes := make([]byte, 1024)
		rnd.Read(dataBytes)

		err := batch.Put(ds.NewKey(fmt.Sprintf("/%s%s", prefix, base32.StdEncoding.EncodeToString(keyBytes))), dataBytes)
		if err != nil {
			return 0, err
		}

		if (i+1)%127 == 0 {
			err := batch.Commit()
			if err != nil {
				return 0, err
			}

			batch, err = r.Datastore().Batch()
			if err != nil {
				return 0, err
			}
		}
	}

	err = batch.Put(ds.NewKey(fmt.Sprintf("/%s%s", prefix, "NOTARANDOMKEY")), []byte("data"))
	if err != nil {
		return 0, err
	}

	err = batch.Commit()
	if err != nil {
		return 0, err
	}

	return seed, nil
}

// Verify checks that keys in the repository look as expected.
func Verify(prefix string, n int, seed int64, r repo.Repo) error {
	rnd := rand.New(rand.NewSource(seed))

	for i := 1; i <= n; i++ {
		keyBytes := make([]byte, 16)
		rnd.Read(keyBytes)
		dataBytes := make([]byte, 1024)
		rnd.Read(dataBytes)

		k := ds.NewKey(fmt.Sprintf("/%s%s", prefix, base32.StdEncoding.EncodeToString(keyBytes)))
		val, err := r.Datastore().Get(k)
		if err != nil {
			return err
		}

		if !bytes.Equal(dataBytes, val) {
			return fmt.Errorf("non-matching data for key %s", k)
		}
	}

	return nil
}
