package trie

import (
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func InitTrieEnv() *Trie {
	db := NewDatabase(rawdb.NewMemoryDatabase())
	tr1, _ := New(emptyRoot, db)
	tr1.Update([]byte("b711355"), []byte("val1"))	
	tr1.Update([]byte("b77d337"), []byte("val2"))
	tr1.Update([]byte("b7f9365"), []byte("val3"))
	tr1.Update([]byte("b77d397"), []byte("val4"))
	rootHash, _ := tr1.Commit(nil)

	// "a711355" -> [6 1 3 7 3 1 3 1 3 3 3 5 3 5 16]
	// "a77d337" -> [6 1 3 7 3 7 6 4 3 3 3 3 3 7 16]
	// "a7f9365" -> [6 1 3 7 6 6 3 9 3 3 3 6 3 5 16]
	// "a77d397" -> [6 1 3 7 3 7 6 4 3 3 3 9 3 7 16]
	tr, _ := New(emptyRoot, db)
	tr.Update([]byte("a711355"), []byte("45"))	
	tr.Update([]byte("a77d337"), []byte("1"))
	tr.Update([]byte("a7f9365"), []byte("1.1"))

	account := Account {
		Root: rootHash,
	}
	val, _ := rlp.EncodeToBytes(account)
	tr.Update([]byte("a77d397"), val)
	tr.Commit(nil)

	return tr
}

func TestTrieStatistics(t *testing.T) {
	inspector, _ := NewInspector(InitTrieEnv(), 10)

	inspector.Do()

	if 1 != inspector.result.StorageCnt {
		t.Fatalf("Test StorageCnt Error")
	}
	if 4 != inspector.result.AccountCnt {
		t.Fatalf("Test AccountCnt Error")
	}
	if 6 != inspector.result.AccountStatis.ShortNodeCnt {
		t.Fatalf("Test AccountStatis.ShortNodeCnt Error")
	}
	if 3 != inspector.result.AccountStatis.FullNodeCnt {
		t.Fatalf("Test AccountStatis.FullNodeCnt Error")
	}
	if 9 != inspector.result.AccountStatis.HashNodeCnt {
		t.Fatalf("Test AccountStatis.HashNodeCnt Error")
	}
}