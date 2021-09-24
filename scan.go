package qexec

type MemScanNode struct {
	Src  []Tuple
	Qual *Qualifier

	currIdx int
}

func (n *MemScanNode) Next() Tuple {
	if n.currIdx == len(n.Src) {
		return nil
	}

	for i := n.currIdx; i < len(n.Src); i++ {
		t := n.Src[n.currIdx]

		n.currIdx++

		if n.Qual != nil {
			if !n.Qual.matches(t) {
				continue
			}
		}

		return t
	}

	return nil
}
