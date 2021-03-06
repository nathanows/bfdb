package executor

type QueryDesc struct {
	PlanHead PlanNode
	Dest     chan Tuple
}

type Tuple map[string]interface{}

type PlanNode interface {
	Next() Tuple
}

func Run(query QueryDesc) {
	dest := query.Dest
	node := query.PlanHead

	if node == nil {
		close(dest)
		return
	}

	for {
		tuple := node.Next()
		if tuple == nil {
			break
		}

		dest <- tuple
	}

	close(dest)
	return
}
