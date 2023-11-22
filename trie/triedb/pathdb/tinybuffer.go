package pathdb

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

var _ trienodebuffer = &tinyBufferList{}

type tinyBuffer struct {
	nb      *nodebuffer
	pre     *tinyBuffer
	next    *tinyBuffer
	stateId uint64
}

func NewTinyBuffer(limit int, nodes map[common.Hash]map[string]*trienode.Node, layers uint64) *tinyBuffer {
	return &tinyBuffer{nb: newNodeBuffer(limit, nodes, layers)}
}

type tinyBufferList struct {
	db    ethdb.KeyValueStore
	clean *fastcache.Cache

	count            uint64
	currLayers       uint64
	currSize         uint64
	backgroundLayers uint64
	backgroundSize   uint64
	limit            uint64
	mux              sync.RWMutex
	stopCh           chan struct{}

	head *tinyBuffer
	tail *tinyBuffer
}

func newTinyBufferList(limit int, nodes map[common.Hash]map[string]*trienode.Node, layers uint64) *tinyBufferList {
	tq := &tinyBufferList{
		limit:  uint64(limit),
		stopCh: make(chan struct{}),
	}
	if nodes != nil {
		tb := NewTinyBuffer(DefaultTinyBufferSize, nodes, layers)
		tq.head = tb
		tq.tail = tb
		tq.currSize = tq.head.nb.size
		tq.currLayers = tq.head.nb.layers
	}
	go tq.asyncTinyNodeBuff()
	return tq
}

func (tq *tinyBufferList) node(owner common.Hash, path []byte, hash common.Hash) (*trienode.Node, error) {
	curr := tq.head
	for {
		if curr == nil {
			return nil, nil
		}
		node, err := curr.nb.node(owner, path, hash)
		if err != nil {
			return nil, err
		}
		if node != nil {
			return node, err
		}
		curr = curr.next
	}
}

func (tq *tinyBufferList) commit(nodes map[common.Hash]map[string]*trienode.Node) trienodebuffer {
	start := time.Now()
	defer func() {
		nodeBufCommitTimeTimer.UpdateSince(start)
	}()
	if tq.head == nil {
		tb := NewTinyBuffer(DefaultTinyBufferSize, nodes, 1)
		tq.head = tb
		tq.tail = tb
		tq.mux.Lock()
		tq.count++
		tq.currSize = tq.head.nb.size
		tq.currLayers = tq.head.nb.layers
		tq.mux.Unlock()
	} else if tq.currSize >= DefaultTinyBufferSize {
		tq.mux.Lock()
		tq.backgroundSize += tq.currSize
		tq.backgroundLayers += tq.currLayers
		tq.mux.Unlock()
		tb := NewTinyBuffer(DefaultTinyBufferSize, nodes, 1)
		tb.next = tq.head
		tq.head.pre = tb
		tq.head = tb
		tq.mux.Lock()
		tq.count++
		tq.currSize = tq.head.nb.size
		tq.currLayers = tq.head.nb.layers
		tq.mux.Unlock()
	} else {
		tq.head.nb.commit(nodes)
		tq.mux.Lock()
		tq.currSize = tq.head.nb.size
		tq.currLayers = tq.head.nb.layers
		tq.mux.Unlock()
	}
	return tq
}

func (tq *tinyBufferList) revert(db ethdb.KeyValueReader, nodes map[common.Hash]map[string]*trienode.Node) error {
	tb := mergeTinyBuffer(tq.head)
	if tb == nil {
		return nil
	}
	tq.head = tb
	tq.tail = tb
	err := tq.head.nb.revert(db, nodes)
	if err != nil {
		return err
	}
	tq.mux.Lock()
	tq.count = 1
	tq.currLayers = tq.head.nb.layers
	tq.currSize = tq.head.nb.size
	tq.backgroundLayers = 0
	tq.backgroundSize = 0
	tq.mux.Unlock()
	return nil
}

func (tq *tinyBufferList) flush(db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64, force bool) error {
	start := time.Now()
	defer func() {
		nodeBufFlushTimeTimer.UpdateSince(start)
	}()
	tq.db = db
	tq.clean = clean
	tq.head.stateId = id
	if force {
		tb := mergeTinyBuffer(tq.head)
		if tb == nil {
			return nil
		}
		return tb.nb.flush(tq.db, tq.clean, tb.stateId, force)
	}
	return nil
}

func (tq *tinyBufferList) setSize(size int, db ethdb.KeyValueStore, clean *fastcache.Cache, id uint64) error {
	return nil
}

func (tq *tinyBufferList) reset() {
	tq.mux.Lock()
	defer tq.mux.Unlock()
	tq.count = 0
	tq.currLayers = 0
	tq.currSize = 0
	tq.backgroundLayers = 0
	tq.backgroundSize = 0
	tq.limit = 0
	tq.head = nil
	tq.tail = nil
}

func (tq *tinyBufferList) empty() bool {
	return tq.head == nil
}

func (tq *tinyBufferList) getSize() (uint64, uint64) {
	tq.mux.Lock()
	defer tq.mux.Unlock()
	return tq.currSize, tq.backgroundSize
}

func (tq *tinyBufferList) getAllNodes() map[common.Hash]map[string]*trienode.Node {
	tb := mergeTinyBuffer(tq.head)
	if tb == nil {
		return nil
	}
	return tb.nb.nodes
}

// getLayers return the size of cached difflayers.
func (tq *tinyBufferList) getLayers() uint64 {
	tq.mux.Lock()
	defer tq.mux.Unlock()
	return tq.currLayers + tq.backgroundLayers
}

func (tq *tinyBufferList) close() {
	close(tq.stopCh)
}

func mergeTinyBuffer(head *tinyBuffer) *tinyBuffer {
	if head == nil {
		return nil
	}
	var (
		curr   = head
		layers uint64
		size   uint64
		nodes  = make(map[common.Hash]map[string]*trienode.Node)
	)

	for {
		if curr == nil {
			break
		}
		for owner, subset := range curr.nb.nodes {
			if _, ok := nodes[owner]; !ok {
				nodes[owner] = make(map[string]*trienode.Node)
			}
			for path, node := range subset {
				nodes[owner][path] = node
			}
		}
		layers += curr.nb.layers
		size += curr.nb.size
		curr = curr.pre
	}
	tb := &tinyBuffer{
		nb: &nodebuffer{
			layers: layers,
			size:   size,
			nodes:  nodes,
			limit:  head.nb.limit,
		},
		stateId: head.stateId,
	}
	return tb
}

func (tq *tinyBufferList) asyncTinyNodeBuff() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			for {
				if tq.tail == nil || tq.tail == tq.head {
					break
				}
				tq.mux.Lock()
				total := tq.currSize + tq.backgroundSize
				tq.mux.Unlock()
				if total < tq.limit {
					break
				}
				if tq.tail.next != nil {
					panic("[BUG] tail tiny node buffer is inaccurate")
				}
				deltaSize := tq.tail.nb.size
				deltaLayers := tq.tail.nb.layers
				tb := tq.tail
				if err := tb.nb.flush(tq.db, tq.clean, tb.stateId, true); err != nil {
					panic("[BUG] failed to flush tail tiny node buffer, err: " + err.Error())
				}
				if tb != tq.tail {
					panic("[BUG] tail tiny node buffer drift")
				}
				tq.tail = tq.tail.pre
				if tq.tail != nil {
					tq.tail.next = nil
				}
				tq.mux.Lock()
				tq.count--
				tq.backgroundSize -= deltaSize
				tq.backgroundLayers -= deltaLayers
				tq.mux.Unlock()
				tq.statistics()
			}
		case <-tq.stopCh:
			return
		}
	}
}

func (tq *tinyBufferList) statistics() {
	tq.mux.Lock()
	defer tq.mux.Unlock()
	log.Info("tiny node buffer info", "front_size", common.StorageSize(tq.currSize),
		"back_size", common.StorageSize(tq.backgroundSize), "count", tq.count, "front_layers",
		tq.currLayers, "back_layers", tq.backgroundLayers)
}
