package qexec

type LimitNode struct {
	Limit int
	Child PlanNode

	retrieved int
}

func (n *LimitNode) Next() Tuple {
	if n.retrieved == n.Limit {
		return nil
	}

	t := n.Child.Next()
	if t == nil {
		return nil
	}

	n.retrieved++

	return t
}
