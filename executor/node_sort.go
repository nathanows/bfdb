package executor

import (
	"log"
	"sort"
)

type SortDir int

const (
	SortAsc SortDir = iota
	SortDesc
)

type SortNode struct {
	Field        string
	Dir          SortDir
	Child        PlanNode
	retrieveDone bool
	res          []Tuple
	resIdx       int
}

func (n *SortNode) Next() Tuple {
	if !n.retrieveDone {
		n.res = []Tuple{}

		if n.Child == nil {
			return nil
		}

		for {
			tup := n.Child.Next()
			if tup == nil {
				n.retrieveDone = true
				break
			}

			n.res = append(n.res, tup)
		}

		if len(n.res) == 0 {
			return nil
		}

		sort.Slice(n.res, func(i, j int) bool {
			var ret bool
			switch n.res[i][n.Field].(type) {
			case string:
				ret = n.res[i][n.Field].(string) < n.res[j][n.Field].(string)
			case int:
				ret = n.res[i][n.Field].(int) < n.res[j][n.Field].(int)
			default:
				log.Fatal("unsupported type")
			}

			if n.Dir == SortDesc {
				return !ret
			}
			return ret
		})
	}

	if len(n.res) == 0 || len(n.res) <= n.resIdx {
		return nil
	}

	tup := n.res[n.resIdx]
	n.resIdx++

	return tup
}
