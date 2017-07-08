package testutil

import (
	repo "github.com/ipfs/go-ipfs/repo"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	"math/rand"

	"fmt"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	"encoding/base32"
)

func OpenRepo(repoPath string) (repo.Repo, error) {
	return fsrepo.Open(repoPath)
}

func InsertRandomKeys(prefix string, n int, r repo.Repo) error {
	batch, err := r.Datastore().Batch()
	if err != nil {
		return err
	}

	for i := 1; i <= n; i++ {
		keyBytes := make([]byte, 16)
		rand.Read(keyBytes)
		dataBytes := make([]byte, 1024)
		rand.Read(dataBytes)

		batch.Put(ds.NewKey(fmt.Sprintf("/%s%s", prefix, base32.StdEncoding.EncodeToString(dataBytes))), dataBytes)
	}

	batch.Commit()

	return nil
}
