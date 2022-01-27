package trie

import (
	"encoding/json"
)
type TrieStatistics struct {
	// logical
	ShortNodeCnt 		uint64				`json:"short_node_cnt"`
	ValueNodeCnt 		uint64				`json:"value_node_cnt"`
	FullNodeCnt 		uint64 				`json:"full_node_cnt"`
	AllNodeCnt 			uint64				`json:"all_node_cnt"`

	AccountCnt 			uint64 				`json:"account_cnt"`
	StorageCnt 			uint64 				`json:"storage_cnt"`
	AccountTopu 		map[uint64]uint64 	`json:"account_tree_topu"`

	// physics
	ShortNodeRlpSize	uint64 				`json:"short_node_rlp_size"`
	FullNodeRlpSize 	uint64 				`json:"full_node_rlp_size"`
	HashNodeCnt 		uint64 				`json:"hash_node_cnt"`
	DiskUsedSize 		uint64 				`json:"disk_used_size"`

	StorageStatis 		*TrieStatistics 	
	AccountStatis 		*TrieStatistics 	
}

func (st *TrieStatistics) Add(other *TrieStatistics) {
	st.ShortNodeCnt += other.ShortNodeCnt
	st.ValueNodeCnt += other.ValueNodeCnt
	st.FullNodeCnt  += other.FullNodeCnt
	st.AllNodeCnt   += other.AllNodeCnt

	st.AccountCnt   += other.AccountCnt

	st.ShortNodeRlpSize += other.ShortNodeRlpSize
	st.FullNodeRlpSize  += other.FullNodeRlpSize
	st.HashNodeCnt      += other.HashNodeCnt
	st.DiskUsedSize     += other.DiskUsedSize
}	

func (st *TrieStatistics) TrieStatisticsMarshal() ([]byte, error) {
	return json.Marshal(&st)
}