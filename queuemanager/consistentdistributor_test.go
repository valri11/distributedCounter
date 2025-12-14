package queuemanager

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_QueueAssignment(t *testing.T) {
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
				"usagedata03",
				"usagedata04",
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
				"0c73cbe51240:18070",
				"f8de62f7219f:18070",
			},
			queues: []string{
				"usagedata01",
				"usagedata02",
			},
		},
	}

	for idx, td := range testData {
		t.Run(fmt.Sprintf("test_%d", idx+1), func(t *testing.T) {

			queueAssignment := make(map[string][]string)

			cd := NewConsistentDistributor()
			cd.Add(td.nodes...)

			for _, q := range td.queues {
				candidateNode := cd.Get(q)

				fmt.Printf("%s - %s\n", q, candidateNode)

				queueAssignment[candidateNode] = append(queueAssignment[candidateNode], q)
			}

			assert.Equal(t, 2, len(queueAssignment))
			for _, queues := range queueAssignment {
				assert.GreaterOrEqual(t, len(queues), 1)
			}
		})
	}
}
