package qexec

import (
	"github.com/fatih/structs"
)

type MemScanNode struct {
	Src  []interface{}
	Expr *Expression
	Proj Projection

	currIdx int
}

func (n *MemScanNode) Next() Tuple {
	if n.currIdx == len(n.Src) {
		return nil
	}

	for i := n.currIdx; i < len(n.Src); i++ {
		t := n.Src[n.currIdx]

		n.currIdx++

		r := buildTuple(t)

		if n.Expr != nil {
			if !n.Expr.Exec(r) {
				continue
			}
		}

		r = n.Proj.Exec(r)

		return r
	}

	return nil
}

func buildTuple(t interface{}) Tuple {
	base := structs.Map(t)
	result := Tuple{}
	for k, v := range base {
		result[ToSnakeCase(k)] = v
	}
	return result
}
