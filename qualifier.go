package qexec

import (
	"fmt"
	"reflect"
)

type QualType int

const (
	QualEql = iota
)

type Qualifier struct {
	Field string
	Type  QualType
	Value string
}

func (q *Qualifier) matches(t Tuple) bool {
	r := reflect.ValueOf(t)
	f := reflect.Indirect(r).FieldByName(q.Field)

	switch q.Type {
	case QualEql:
		return fmt.Sprintf("%v", f) == q.Value
	default:
		return false
	}
}
