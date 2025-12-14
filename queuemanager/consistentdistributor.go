package queuemanager

import (
	"hash/crc32"
	"math"
	"sort"
)

type ConsistentDistributor struct {
	keys      []int
	keyToNode map[int]string
	nodeLoad  map[string]int
	minLoad   int
	hashFn    func(data []byte) uint32
}

func NewConsistentDistributor() *ConsistentDistributor {
	cd := ConsistentDistributor{
		keyToNode: make(map[int]string),
		nodeLoad:  make(map[string]int),
		hashFn:    crc32.ChecksumIEEE,
	}
	return &cd
}

func (cd *ConsistentDistributor) IsEmpty() bool {
	return len(cd.keys) == 0
}

func (cd *ConsistentDistributor) Add(keys ...string) {
	for _, node := range keys {
		key := int(cd.hashFn([]byte(node)))
		cd.keys = append(cd.keys, key)
		cd.keyToNode[key] = node
	}
	sort.Ints(cd.keys)
}

func (cd *ConsistentDistributor) Get(key string) string {
	if cd.IsEmpty() {
		return ""
	}

	hash := int(cd.hashFn([]byte(key)))

	idx := sort.Search(len(cd.keys), func(i int) bool {
		return cd.keys[i] >= hash
	})

	// Means we have cycled back to the first replica.
	if idx == len(cd.keys) {
		idx = 0
	}

	candidateNode := cd.keyToNode[cd.keys[idx]]

	load := cd.nodeLoad[candidateNode]
	if load >= cd.minLoad {
		newCandidateNode := candidateNode
		j := idx + 1
		if j == len(cd.keys) {
			j = 0
		}
		for ; j < len(cd.keys); j++ {
			newCandidateNode = cd.keyToNode[cd.keys[j]]

			if cd.nodeLoad[newCandidateNode] <= cd.minLoad {
				break
			}
		}
		candidateNode = newCandidateNode
	}

	cd.nodeLoad[candidateNode]++

	newMinLoad := math.MaxInt
	for _, v := range cd.nodeLoad {
		newMinLoad = min(newMinLoad, v)
	}
	cd.minLoad = newMinLoad

	return candidateNode
}
