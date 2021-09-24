package qexec

import "fmt"

type AggType int

const (
	AggSum AggType = iota
	AggCount
)

type AggNode struct {
	Type    AggType
	Field   string
	Proj    Projection
	Child   PlanNode
	aggDone bool
	sum     interface{}
	count   int
}

func (n *AggNode) Next() Tuple {
	if n.aggDone {
		return nil
	}

	if n.Child == nil {
		return nil
	}

	for {
		tup := n.Child.Next()
		if tup == nil {
			break
		}

		n.count++

		switch tup[n.Field].(type) {
		case int:
			if n.sum == nil {
				n.sum = int(0)
			}
			n.sum = n.sum.(int) + tup[n.Field].(int)
		case float64:
			if n.sum == nil {
				n.sum = float64(0)
			}
			n.sum = n.sum.(float64) + tup[n.Field].(float64)
		}
	}

	n.aggDone = true

	var rt Tuple
	switch n.Type {
	case AggSum:
		rt = map[string]interface{}{fmt.Sprintf("sum(%s)", ToSnakeCase(n.Field)): n.sum}
	case AggCount:
		rt = map[string]interface{}{fmt.Sprintf("count(%s)", ToSnakeCase(n.Field)): n.count}
	default:
	}

	return n.Proj.Exec(rt)
}
