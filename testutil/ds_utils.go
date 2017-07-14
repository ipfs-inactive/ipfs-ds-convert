package testutil

import (
	crand "crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"math/rand"

	repo "gx/ipfs/QmV4cdHmCmWwqfjPnS55C3hArsXSyYyQeY8F6tsyL6J1L8/go-ipfs/repo"
	fsrepo "gx/ipfs/QmV4cdHmCmWwqfjPnS55C3hArsXSyYyQeY8F6tsyL6J1L8/go-ipfs/repo/fsrepo"

	"bytes"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
)

func OpenRepo(repoPath string) (repo.Repo, error) {
	return fsrepo.Open(repoPath)
}

func getSeed() int64 {
	b := make([]byte, 8)
	crand.Read(b)
	return int64(binary.LittleEndian.Uint64(b))
}

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

	err = batch.Commit()
	if err != nil {
		return 0, err
	}

	return seed, nil
}

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

		if !bytes.Equal(dataBytes, val.([]byte)) {
			return fmt.Errorf("Non-matching data for key %s", k)
		}
	}

	return nil
}
