package executor

import "fmt"

type UniqNode struct {
	Field       string
	Child       PlanNode
	initialized bool
	res         map[string]Tuple
}

func (n *UniqNode) Next() Tuple {
	if n.Child == nil {
		return nil
	}

	if !n.initialized {
		n.res = map[string]Tuple{}
		n.initialized = true
	}

	for {
		tup := n.Child.Next()
		if tup == nil {
			return nil
		}

		k := fmt.Sprintf("%v", tup[n.Field])
		if _, ok := n.res[k]; ok {
			continue
		}

		n.res[k] = tup
		return tup
	}
}
