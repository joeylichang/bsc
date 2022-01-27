package trie 

import (
	"os"
	"fmt"
	"time"
	"bytes"
	"errors"
	"strconv"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/common"
)

const (
	DEFAULT_STATISTICS_PATH  = "trienode_stistics.json"
	DEFAULT_TRIEDBCACHE_SIZE = 1024 * 1024 * 1024 
)

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}

type Inspector struct {
	trie  		*Trie 					// traverse trie
	root 		node  					// root of triedb
	num 		uint64 					// block number
	file 		string 					// result storage path
	result 		*TrieStatistics 		// inspector result
	accountCh 	chan *TrieStatistics 	// chan for account task result return
	storageCh 	chan *TrieStatistics	// chan for sub storage task result return

	pending  	int64 					// pending tasks number
}

// NewInspector return a inspector obj
func NewInspector(tr *Trie, num uint64) (*Inspector, error) {
	if tr == nil {
		return nil, errors.New("trie is nil") 
	}

	if tr.root == nil {
		return nil, errors.New("trie root is nil") 
	}

	file := "./" + strconv.FormatUint(num, 10) + "_" + DEFAULT_STATISTICS_PATH
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
		    if _, err = os.Create(file); err != nil {
		    	return nil, errors.New(fmt.Sprintf("create file %s err: %v", file, err))
		    }
		} else {
			return nil, errors.New(fmt.Sprintf("check file %s err: %v", file, err))
		}
	}

	ins := &Inspector {
		trie: 		tr,
		root:  		tr.root,
		num: 		num,
		file:		file, 		
		result:		&TrieStatistics{
			AccountTopu:	make(map[uint64]uint64),
			AccountStatis:	&TrieStatistics{ AccountTopu: make(map[uint64]uint64), },
			StorageStatis:	&TrieStatistics{ AccountTopu: make(map[uint64]uint64), },
		},
		accountCh:	make(chan *TrieStatistics, 1),
		storageCh:	make(chan *TrieStatistics, 100),
		pending:  	0,
	}

	return ins, nil
}

// Do start statistics, external call
func (ins *Inspector) Do() error {
	// do account statistics
	atomic.AddInt64(&ins.pending, int64(1))
	go ins.runTask(ins.root, ins.result.AccountStatis, ins.trie, ins.accountCh)

	timeTicker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <- ins.accountCh :
			// account statistics done
			atomic.AddInt64(&ins.pending, int64(-1))
		case storeSubRes := <- ins.storageCh:
			// sub storage statistics done
			atomic.AddInt64(&ins.pending, int64(-1))
			ins.result.StorageCnt++
			ins.result.StorageStatis.Add(storeSubRes)
		case <- timeTicker.C:
			ins.trie.db.Cap(DEFAULT_TRIEDBCACHE_SIZE)
		}
		// // all done
		if atomic.LoadInt64(&ins.pending) == 0 {
			break
		}
	}
	return ins.finalize()
}

// runTask block goroutine wait run accountTask or storageTask
func (ins *Inspector) runTask(root node, statis *TrieStatistics, tr *Trie, resCh chan *TrieStatistics) {
	var height uint64
	path := make([]byte, 0)
	if statis == nil {
		statis = &TrieStatistics{ AccountTopu:	make(map[uint64]uint64), }
	}
	resCh <- ins.runSubTask(root, statis, tr, height, path)
}

// runSubTask do sub statistics
func (ins *Inspector) runSubTask(root node, statis *TrieStatistics, tr *Trie, height uint64, path []byte) *TrieStatistics {
	if root == nil {
		return statis
	}

	switch current := (root).(type) {
		case *shortNode:
			path = append(path, current.Key...)
			ins.runSubTask(current.Val, statis, tr, height + 1, path)
			path = path[:len(path) - len(current.Key)]
		case *fullNode:
			for idx, child := range current.Children {
				if child == nil {
					continue 
				}
				childPath := path
				childPath = append(childPath, byte(idx))
				ins.runSubTask(child, statis, tr, height + 1, childPath)
			}
		case hashNode:
			n, err := tr.resolveHash(current, nil)
			if err != nil {
				fmt.Println(fmt.Sprintf("resolveHash HashNode %v error: %v", current, err))
				return statis
			}
			return ins.runSubTask(n, statis, tr, height, path)
		case valueNode:
			statis.AccountCnt++
			if !hasTerm(path) {
				break
			}

			var account Account
			if err := rlp.Decode(bytes.NewReader(current), &account); err != nil {
				break
			}

			if account.Root == (common.Hash{}) || account.Root == emptyRoot {
				break
			}

			storageTr, err := New(account.Root, tr.db)
		    if err != nil {
		    	fmt.Println(fmt.Sprintf("new storage triee node %v error: %v", current, err))
		    	break
		    }
		    // Notice: sub storage trie no cap
		    atomic.AddInt64(&ins.pending, int64(1))
			go ins.runTask(storageTr.root, nil, storageTr, ins.storageCh)
		default:
			panic(errors.New("invalid node type to traverse"))
	}

	ins.doOneNodeStatistics(statis, tr, root, height)
	return statis
}

func (ins *Inspector) doOneNodeStatistics(statis *TrieStatistics, tr *Trie, n node, height uint64) {
	statis.AccountTopu[height]++
	statis.AllNodeCnt++

	var rlpSize uint64
	hash, _ := n.cache()
	if hash != nil {
		enc, err := tr.db.diskdb.Get(hash[:])
		if err == nil || enc != nil {
			rlpSize = uint64(len(enc))
		}
	}

	statis.DiskUsedSize += rlpSize
	statis.DiskUsedSize += common.HashLength
	switch (n).(type) {
		case *shortNode:
			statis.ShortNodeRlpSize += rlpSize
			statis.ShortNodeCnt++
			statis.HashNodeCnt++
		case *fullNode:
			statis.FullNodeRlpSize += rlpSize
			statis.FullNodeCnt++
			statis.HashNodeCnt++
		case valueNode:
			statis.ValueNodeCnt++
		case hashNode:
			panic(errors.New("hashNode should not statistics"))
		default:
			panic(errors.New("invalid node type to statistics"))
	}
}

// finalize merge sub statistics to result，and write disk
func (ins *Inspector) finalize() error {
	ins.result.StorageStatis.AccountCnt   = 0
	ins.result.StorageStatis.AccountTopu  = make(map[uint64]uint64)
	ins.result.StorageStatis.DiskUsedSize = ins.result.StorageStatis.HashNodeCnt * common.HashLength + ins.result.StorageStatis.ShortNodeRlpSize + ins.result.StorageStatis.FullNodeRlpSize
	ins.result.AccountStatis.DiskUsedSize = ins.result.AccountStatis.HashNodeCnt * common.HashLength + ins.result.AccountStatis.ShortNodeRlpSize + ins.result.AccountStatis.FullNodeRlpSize
	ins.result.Add(ins.result.StorageStatis)
	ins.result.Add(ins.result.AccountStatis)

	f, err := os.OpenFile(ins.file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.New(fmt.Sprintf("open file %s err: %v", ins.file, err))
	}
	defer f.Close()
	_, err = f.WriteString(fmt.Sprintf("Block Number: %v\n", ins.num))

	totalData, err   := ins.result.TrieStatisticsMarshal()
	_, err = f.WriteString("Total Statistics:\n")
	_, err = f.WriteString(string(totalData) + "\n")

	accountData, err := ins.result.AccountStatis.TrieStatisticsMarshal()
	_, err = f.WriteString("Account Statistics:\n")
	_, err = f.WriteString(string(accountData) + "\n")

	storageData, err := ins.result.StorageStatis.TrieStatisticsMarshal()
	_, err = f.WriteString("Storage Statistics:\n")
	_, err = f.WriteString(string(storageData) + "\n\n")

	if err != nil {
		fmt.Println(fmt.Sprintf("Json Marshal or Write disk error: %v", err))
		return err
	}

	return nil
}