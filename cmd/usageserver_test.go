package cmd

import (
	"fmt"
	"hash/crc32"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/cespare/xxhash"
	"github.com/golang/groupcache/consistenthash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chash "github.com/nobound/go-consistent"

	ch3 "github.com/lafikl/consistent"

	ch4 "github.com/arriqaaq/xring"

	"github.com/buraksezer/consistent"
)

// Input: M queues (M > 0), N nodes (N > 0)
// Requirements: each queue should have at least one node assigned
// if M < N - create additional queues

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type nodeMember string

func (m nodeMember) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func Test_QueueAssignment_1(t *testing.T) {
	testData := []struct {
		nodes  []string
		queues []string
	}{
		{
			nodes: []string{
				"26f9dfba626e:18070",
				"82750ff31367:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"0df3fbb6b8f0:18070",
				"bb00d1381661:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"0c73cbe51240:18070",
				"f8de62f7219f:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"402747bc58cb:18070",
				"c3778424688c:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
	}

	for idx, td := range testData {
		t.Run(fmt.Sprintf("test_%d", idx+1), func(t *testing.T) {
			cfg := consistent.Config{
				//PartitionCount:    7,
				//ReplicationFactor: 20,
				//Load:              1.25,
				Hasher: hasher{},
			}
			c := consistent.New(nil, cfg)

			for _, node := range td.nodes {
				c.Add(nodeMember(node))
			}

			queueAssignment := make(map[string][]string)

			for _, q := range td.queues {
				key := []byte(q)
				owner := c.LocateKey(key)
				node := owner.String()
				fmt.Printf("%s - %s\n", q, node)

				queueAssignment[node] = append(queueAssignment[node], q)
			}

			fmt.Printf("load distributions: %v\n", c.LoadDistribution())

			assert.Equal(t, 2, len(queueAssignment))
			for _, queues := range queueAssignment {
				assert.GreaterOrEqual(t, len(queues), 1)
			}
		})
	}
}

func Test_QueueAssignment_2(t *testing.T) {
	testData := []struct {
		nodes  []string
		queues []string
	}{
		{
			nodes: []string{
				"402747bc58cb:18070",
				"c3778424688c:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
				"usagedata03",
			},
		},
		{
			nodes: []string{
				"402747bc58cb:18070",
				"c3778424688c:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"26f9dfba626e:18070",
				"82750ff31367:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"0df3fbb6b8f0:18070",
				"bb00d1381661:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"0c73cbe51240:18070",
				"f8de62f7219f:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
	}

	hashFn := crc32.ChecksumIEEE

	for idx, td := range testData {
		t.Run(fmt.Sprintf("test_%d", idx+1), func(t *testing.T) {

			queueAssignment := make(map[string][]string)

			var keys []int
			keyToNode := make(map[int]string)
			nodeLoad := make(map[string]int)

			for _, node := range td.nodes {
				key := int(hashFn([]byte(node)))
				keys = append(keys, key)
				keyToNode[key] = node
			}
			sort.Ints(keys)

			fill := 1
			for _, q := range td.queues {

				hash := int(hashFn([]byte(q)))

				idx := sort.Search(len(keys), func(i int) bool {
					return keys[i] >= hash
				})

				// Means we have cycled back to the first replica.
				if idx == len(keys) {
					idx = 0
				}

				candidateNode := keyToNode[keys[idx]]

				load := nodeLoad[candidateNode]
				if load >= fill {
					newCandidateNode := candidateNode
					j := idx + 1
					if j == len(keys) {
						j = 0
					}
					for ; j < len(keys); j++ {
						newCandidateNode = keyToNode[keys[j]]

						if nodeLoad[newCandidateNode] < fill {
							break
						}
					}
					candidateNode = newCandidateNode
				}

				fmt.Printf("%s - %s\n", q, candidateNode)

				queueAssignment[candidateNode] = append(queueAssignment[candidateNode], q)
				nodeLoad[candidateNode]++
			}

			assert.Equal(t, 2, len(queueAssignment))
			for _, queues := range queueAssignment {
				assert.GreaterOrEqual(t, len(queues), 1)
			}
		})
	}

	/*


		nodes := []string{
			"0df3fbb6b8f0:18070",
			"bb00d1381661:18070",
		}

		queues := []string{}

		queueAssignment := make(map[string][]string)


	*/
}

func Test_QueueAssignment_3(t *testing.T) {
	testData := []struct {
		nodes  []string
		queues []string
	}{
		{
			nodes: []string{
				"0df3fbb6b8f0:18070",
				"bb00d1381661:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"0c73cbe51240:18070",
				"f8de62f7219f:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"26f9dfba626e:18070",
				"82750ff31367:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"402747bc58cb:18070",
				"c3778424688c:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
	}

	for idx, td := range testData {
		t.Run(fmt.Sprintf("test_%d", idx+1), func(t *testing.T) {
			ch := ch3.New()

			for _, node := range td.nodes {
				ch.Add(node)
			}

			queueAssignment := make(map[string][]string)

			for _, q := range td.queues {
				//node := ch.Get(q)
				node, err := ch.GetLeast(q)
				require.NoError(t, err)

				fmt.Printf("%s - %s\n", q, node)

				queueAssignment[node] = append(queueAssignment[node], q)

				ch.Inc(node)
				//ch.Done(node)
			}

			assert.Equal(t, 2, len(queueAssignment))
			for _, queues := range queueAssignment {
				assert.GreaterOrEqual(t, len(queues), 1)
			}
		})
	}
}

func Test_QueueAssignment_4(t *testing.T) {
	testData := []struct {
		nodes  []string
		queues []string
	}{
		{
			nodes: []string{
				"0df3fbb6b8f0:18070",
				"bb00d1381661:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"0c73cbe51240:18070",
				"f8de62f7219f:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"26f9dfba626e:18070",
				"82750ff31367:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
		{
			nodes: []string{
				"402747bc58cb:18070",
				"c3778424688c:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
	}

	for idx, td := range testData {
		t.Run(fmt.Sprintf("test_%d", idx+1), func(t *testing.T) {
			cnf := &ch4.Config{
				VirtualNodes: 50,
				LoadFactor:   1,
			}
			ch := ch4.NewRing(td.nodes, cnf)

			queueAssignment := make(map[string][]string)

			for _, q := range td.queues {
				//node := ch.Get(q)
				node, err := ch.Get(q)
				require.NoError(t, err)

				fmt.Printf("%s - %s\n", q, node)

				queueAssignment[node] = append(queueAssignment[node], q)
			}

			assert.Equal(t, 2, len(queueAssignment))
			for _, queues := range queueAssignment {
				assert.GreaterOrEqual(t, len(queues), 1)
			}
		})
	}
}

func Test_1(t *testing.T) {
	node := ServiceNode{}
	node.IsLeader = true

	const replicas = 50
	ch := consistenthash.New(replicas, nil)

	/*
		members := []string{
			"0df3fbb6b8f0:18070",
			"bb00d1381661:18070",
		}
	*/

	/*
		members := []string{
			"0c73cbe51240:18070",
			"f8de62f7219f:18070",
		}
	*/

	members := []string{
		"402747bc58cb:18070",
		"c3778424688c:18070",
	}

	existingQueues := []string{
		"usagedata01",
		"usagedata02",
	}

	for _, m := range members {
		ch.Add(m)
	}

	slices.Sort(existingQueues)

	// each node will get at least one queue

	node.QueueAssignment = make(map[string][]string)

	queueToNodes := make(map[string][]string)
	nodeToQueues := make(map[string][]string)

	for _, queueName := range existingQueues {
		// get node for queue
		nodeName := ch.Get(queueName)

		fmt.Printf("assign queue. node: %s, queue: %s\n", nodeName, queueName)

		nodeToQueues[nodeName] = append(nodeToQueues[nodeName], queueName)
		queueToNodes[queueName] = append(queueToNodes[queueName], nodeName)
	}

	for idx, node := range members {
		if queues, ok := nodeToQueues[node]; ok {
			if len(queues) > 0 {
				// has at least one queue assigned
				continue
			}
		}

		queueName := fmt.Sprintf("%s%02d", "usagedata", idx+1)
		if qn, ok := queueToNodes[queueName]; ok {
			if len(qn) > 0 {
				// has at least one node assigned
				continue
			}
		}
		// get node for queue
		nodeName := ch.Get(queueName)

		slog.Debug("assign queue", "node", nodeName, "queue", queueName)

		nodeToQueues[nodeName] = append(nodeToQueues[nodeName], queueName)
	}

	// sort and remove dups
	for _, m := range members {
		queues := nodeToQueues[m]
		slices.Sort(queues)
		queues = slices.Compact(queues)
		nodeToQueues[m] = queues
	}
	node.QueueAssignment = nodeToQueues

	slog.Debug("service state", "state", node)
}

func Test_2(t *testing.T) {
	node := ServiceNode{}
	node.IsLeader = true

	const replicas = 50
	ch := chash.New(chash.Config{
		ReplicationFactor: replicas,
	})

	/*
		members := []string{
			"0df3fbb6b8f0:18070",
			"bb00d1381661:18070",
		}
	*/

	/*
		members := []string{
			"0c73cbe51240:18070",
			"f8de62f7219f:18070",
		}
	*/

	members := []string{
		"402747bc58cb:18070",
		"c3778424688c:18070",
	}

	existingQueues := []string{
		"usagedata01",
		"usagedata02",
	}

	for _, m := range members {
		ch.Add(m)
	}

	fmt.Printf("nodes: %v\n", ch.GetNodeNames())

	slices.Sort(existingQueues)

	// each node will get at least one queue

	node.QueueAssignment = make(map[string][]string)

	queueToNodes := make(map[string][]string)
	nodeToQueues := make(map[string][]string)

	for _, queueName := range existingQueues {
		// get node for queue
		nodeName := ch.Get(queueName)

		fmt.Printf("assign queue. node: %s, queue: %s\n", nodeName, queueName)

		nodeToQueues[nodeName] = append(nodeToQueues[nodeName], queueName)
		queueToNodes[queueName] = append(queueToNodes[queueName], nodeName)
	}

	for idx, node := range members {
		if queues, ok := nodeToQueues[node]; ok {
			if len(queues) > 0 {
				// has at least one queue assigned
				continue
			}
		}

		queueName := fmt.Sprintf("%s%02d", "usagedata", idx+1)
		if qn, ok := queueToNodes[queueName]; ok {
			if len(qn) > 0 {
				// has at least one node assigned
				continue
			}
		}
		// get node for queue
		nodeName := ch.Get(queueName)

		slog.Debug("assign queue", "node", nodeName, "queue", queueName)

		nodeToQueues[nodeName] = append(nodeToQueues[nodeName], queueName)
	}

	// sort and remove dups
	for _, m := range members {
		queues := nodeToQueues[m]
		slices.Sort(queues)
		queues = slices.Compact(queues)
		nodeToQueues[m] = queues
	}
	node.QueueAssignment = nodeToQueues

	slog.Debug("service state", "state", node)
}
