// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package pathdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	bloomfilter "github.com/holiman/bloomfilter/v2"
)

var (
	// aggregatorMemoryLimit is the maximum size of the bottom-most diff layer
	// that aggregates the writes from above until it's flushed into the disk
	// layer.
	//
	// Note, bumping this up might drastically increase the size of the bloom
	// filters that's stored in every diff layer. Don't do that without fully
	// understanding all the implications.
	aggregatorMemoryLimit = uint64(4 * 1024 * 1024)

	// aggregatorItemLimit is an approximate number of items that will end up
	// in the agregator layer before it's flushed out to disk. A plain account
	// weighs around 14B (+hash), a storage slot 32B (+hash), a deleted slot
	// 0B (+hash). Slots are mostly set/unset in lockstep, so that average at
	// 16B (+hash). All in all, the average entry seems to be 15+32=47B. Use a
	// smaller number to be on the safe side.
	aggregatorItemLimit = aggregatorMemoryLimit / 42

	// bloomTargetError is the target false positive rate when the aggregator
	// layer is at its fullest. The actual value will probably move around up
	// and down from this number, it's mostly a ballpark figure.
	//
	// Note, dropping this down might drastically increase the size of the bloom
	// filters that's stored in every diff layer. Don't do that without fully
	// understanding all the implications.
	bloomTargetError = 0.02

	// bloomSize is the ideal bloom filter size given the maximum number of items
	// it's expected to hold and the target false positive error rate.
	bloomSize = math.Ceil(float64(aggregatorItemLimit) * math.Log(bloomTargetError) / math.Log(1/math.Pow(2, math.Log(2))))

	// bloomFuncs is the ideal number of bits a single entry should set in the
	// bloom filter to keep its size to a minimum (given it's size and maximum
	// entry count).
	bloomFuncs = math.Round((bloomSize / float64(aggregatorItemLimit)) * math.Log(2))

	// the bloom offsets are runtime constants which determines which part of the
	// account/storage hash the hasher functions looks at, to determine the
	// bloom key for an account/slot. This is randomized at init(), so that the
	// global population of nodes do not all display the exact same behaviour with
	// regards to bloom content
	bloomAccountHasherOffset = 0
	bloomStorageHasherOffset = 0
)

func init() {
	// Init the bloom offsets in the range [0:24] (requires 8 bytes)
	bloomAccountHasherOffset = rand.Intn(25)
	bloomStorageHasherOffset = rand.Intn(25)
}

// pathBloomHasher is a wrapper to satisfy the interface API requirements of the
// bloom library used. It's used to convert an account hash into a 64 bit mini hash.
type pathBloomHasher struct {
	owner common.Hash
	node  common.Hash
}

func NewPathBloomHasher(owner common.Hash, node common.Hash) *pathBloomHasher {
	return &pathBloomHasher{
		owner: owner,
		node:  node,
	}
}

func (h *pathBloomHasher) Write(p []byte) (n int, err error) { panic("not implemented") }
func (h *pathBloomHasher) Sum(b []byte) []byte               { panic("not implemented") }
func (h *pathBloomHasher) Reset()                            { panic("not implemented") }
func (h *pathBloomHasher) BlockSize() int                    { panic("not implemented") }
func (h *pathBloomHasher) Size() int                         { return 8 }
func (h *pathBloomHasher) Sum64() uint64 {
	if h.owner == (common.Hash{}) {
		return binary.BigEndian.Uint64(h.node[bloomAccountHasherOffset : bloomAccountHasherOffset+8])
	}
	return binary.BigEndian.Uint64(h.owner[bloomStorageHasherOffset:bloomStorageHasherOffset+8]) ^
		binary.BigEndian.Uint64(h.node[bloomStorageHasherOffset:bloomStorageHasherOffset+8])
}

// diffLayer represents a collection of modifications made to the in-memory tries
// along with associated state changes after running a block on top.
//
// The goal of a diff layer is to act as a journal, tracking recent modifications
// made to the state, that have not yet graduated into a semi-immutable state.
type diffLayer struct {
	// Immutables
	root   common.Hash                               // Root hash to which this layer diff belongs to
	id     uint64                                    // Corresponding state id
	block  uint64                                    // Associated block number
	nodes  map[common.Hash]map[string]*trienode.Node // Cached trie nodes indexed by owner and path
	states *triestate.Set                            // Associated state change set for building history
	memory uint64                                    // Approximate guess as to how much memory we use

	origin        *diskLayer
	currentDiffed *bloomfilter.Filter // Bloom filter tracking all the diffed items belong to current layer
	parentsDiffed *bloomfilter.Filter // Bloom filter tracking all the diffed items up to the disk layer

	parent layer        // Parent layer modified by this one, never nil, **can be changed**
	lock   sync.RWMutex // Lock used to protect parent
}

// newDiffLayer creates a new diff layer on top of an existing layer.
func newDiffLayer(parent layer, root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	var (
		size  int64
		count int
	)
	dl := &diffLayer{
		root:   root,
		id:     id,
		block:  block,
		nodes:  nodes,
		states: states,
		parent: parent,
	}
	diffed, err := bloomfilter.New(uint64(bloomSize), uint64(bloomFuncs))
	if err != nil {
		log.Warn("failed to init difflayer bloom filter", "error", err)
	}
	if diffed != nil {
		for owner, subset := range nodes {
			for path, n := range subset {
				dl.memory += uint64(n.Size() + len(path))
				size += int64(len(n.Blob) + len(path))
				diffed.Add(NewPathBloomHasher(owner, n.Hash))
			}
			count += len(subset)
		}
	}
	dl.currentDiffed = diffed
	//dl.parentsDiffed = diffed
	//if diffed != nil {
	//	if par, ok := dl.parent.(*diffLayer); ok {
	//		mergedBloom, err := par.parentsDiffed.Union(dl.currentDiffed)
	//		if err != nil {
	//			log.Error("failed to merge bloom filter", "parent", par.block, "child", dl.block, "error", err)
	//		} else {
	//			dl.parentsDiffed = mergedBloom
	//		}
	//	}
	//}

	if states != nil {
		dl.memory += uint64(states.Size())
	}
	dirtyWriteMeter.Mark(size)
	diffLayerNodesMeter.Mark(int64(count))
	diffLayerBytesMeter.Mark(int64(dl.memory))
	log.Debug("Created new diff layer", "id", id, "block", block, "nodes", count, "size", common.StorageSize(dl.memory), "root", dl.root)
	return dl
}

// rootHash implements the layer interface, returning the root hash of
// corresponding state.
func (dl *diffLayer) rootHash() common.Hash {
	return dl.root
}

// stateID implements the layer interface, returning the state id of the layer.
func (dl *diffLayer) stateID() uint64 {
	return dl.id
}

// parentLayer implements the layer interface, returning the subsequent
// layer of the diff layer.
func (dl *diffLayer) parentLayer() layer {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.parent
}

// rebloom discards the layer's current bloom and rebuilds it from scratch based
// on the parent's and the local diffs.
func (dl *diffLayer) rebloom(origin *diskLayer) {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	// Inject the new origin that triggered the rebloom
	dl.origin = origin

	// Retrieve the parent bloom or create a fresh empty one
	if parent, ok := dl.parent.(*diffLayer); ok {
		parent.lock.RLock()
		dl.parentsDiffed, _ = parent.parentsDiffed.Union(dl.currentDiffed)
		parent.lock.RUnlock()
	} else {
		dl.parentsDiffed, _ = dl.currentDiffed.Copy()
	}
}

// node retrieves the node with provided node information. It's the internal
// version of Node function with additional accessed layer tracked. No error
// will be returned if node is not found.
func (dl *diffLayer) node(owner common.Hash, path []byte, hash common.Hash, pathHash *pathBloomHasher, depth int) ([]byte, error) {
	// Hold the lock, ensure the parent won't be changed during the
	// state accessing.
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	//if dl.parentsDiffed != nil && !dl.parentsDiffed.Contains(pathHash) {
	//	if dl.origin != nil {
	//		return dl.origin.Node(owner, path, hash)
	//	}
	//	parentLayer := dl.parent
	//	for {
	//		if disk, ok := parentLayer.(*diskLayer); ok {
	//			return disk.Node(owner, path, hash)
	//		}
	//		parentLayer = parentLayer.parentLayer()
	//	}
	//}
	//dirtyBloomHitMeter.Mark(1)

	if dl.currentDiffed != nil && !dl.currentDiffed.Contains(pathHash) {
		// Trie node unknown to this layer, resolve from parent
		if diff, ok := dl.parent.(*diffLayer); ok {
			return diff.node(owner, path, hash, pathHash, depth+1)
		}
		// Failed to resolve through diff layers, fallback to disk layer
		return dl.parent.Node(owner, path, hash)
	}

	// If the trie node is known locally, return it
	subset, ok := dl.nodes[owner]
	if ok {
		n, ok := subset[string(path)]
		if ok {
			// If the trie node is not hash matched, or marked as removed,
			// bubble up an error here. It shouldn't happen at all.
			if n.Hash != hash {
				dirtyFalseMeter.Mark(1)
				log.Error("Unexpected trie node in diff layer", "owner", owner, "path", path, "expect", hash, "got", n.Hash, "depth", depth)
				return nil, newUnexpectedNodeError("diff", hash, n.Hash, owner, path, n.Blob)
			}
			dirtyHitMeter.Mark(1)
			dirtyNodeHitDepthHist.Update(int64(depth))
			dirtyReadMeter.Mark(int64(len(n.Blob)))
			return n.Blob, nil
		}
	}
	// Trie node unknown to this layer, resolve from parent
	if diff, ok := dl.parent.(*diffLayer); ok {
		return diff.node(owner, path, hash, pathHash, depth+1)
	}
	// Failed to resolve through diff layers, fallback to disk layer
	return dl.parent.Node(owner, path, hash)
}

// Node implements the layer interface, retrieving the trie node blob with the
// provided node information. No error will be returned if the node is not found.
func (dl *diffLayer) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return dl.node(owner, path, hash, NewPathBloomHasher(owner, hash), 0)
}

// update implements the layer interface, creating a new layer on top of the
// existing layer tree with the specified data items.
func (dl *diffLayer) update(root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	return newDiffLayer(dl, root, id, block, nodes, states)
}

// persist flushes the diff layer and all its parent layers to disk layer.
func (dl *diffLayer) persist(force bool) (layer, error) {
	if parent, ok := dl.parentLayer().(*diffLayer); ok {
		// Hold the lock to prevent any read operation until the new
		// parent is linked correctly.
		dl.lock.Lock()

		// The merging of diff layers starts at the bottom-most layer,
		// therefore we recurse down here, flattening on the way up
		// (diffToDisk).
		result, err := parent.persist(force)
		if err != nil {
			dl.lock.Unlock()
			return nil, err
		}
		dl.parent = result
		dl.lock.Unlock()
	}
	return diffToDisk(dl, force)
}

// diffToDisk merges a bottom-most diff into the persistent disk layer underneath
// it. The method will panic if called onto a non-bottom-most diff layer.
func diffToDisk(layer *diffLayer, force bool) (layer, error) {
	disk, ok := layer.parentLayer().(*diskLayer)
	if !ok {
		panic(fmt.Sprintf("unknown layer type: %T", layer.parentLayer()))
	}
	return disk.commit(layer, force)
}
