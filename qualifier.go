package qexec

type QualType int

const (
	QualEql QualType = iota
)

type Qualifier struct {
	Field string
	Type  QualType
	Value interface{}
}

func (q *Qualifier) matches(t Tuple) bool {
	switch q.Type {
	case QualEql:
		return t[q.Field] == q.Value
	default:
		return false
	}
}
