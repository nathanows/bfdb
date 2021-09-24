package qexec

import (
	"reflect"
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

		fieldType := reflect.Indirect(reflect.ValueOf(n.res[0])).FieldByName(n.Field).Kind()

		sort.Slice(n.res, func(i, j int) bool {
			v1 := reflect.Indirect(reflect.ValueOf(n.res[i])).FieldByName(n.Field)
			v2 := reflect.Indirect(reflect.ValueOf(n.res[j])).FieldByName(n.Field)

			ret := false

			switch fieldType {
			case reflect.Int64:
				ret = int64(v1.Int()) < int64(v2.Int())
			case reflect.Float64:
				ret = float64(v1.Float()) < float64(v2.Float())
			case reflect.String:
				ret = string(v1.String()) < string(v2.String())
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
