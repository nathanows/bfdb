package qexec

type ExpressionType int

const (
	ExprAnd ExpressionType = iota
	ExprOr  ExpressionType = iota
)

type Expression struct {
	Type  ExpressionType
	Qual  *Qualifier
	Left  *Expression
	Right *Expression
}

func (e *Expression) Exec(t Tuple) bool {
	if e.Left == nil && e.Right == nil {
		if e.Qual == nil {
			return true
		}
		return e.Qual.matches(t)
	}

	if e.Right == nil {
		return e.Left.Exec(t)
	}

	if e.Left == nil {
		return e.Right.Exec(t)
	}

	l := e.Left.Exec(t)
	r := e.Right.Exec(t)

	switch e.Type {
	case ExprAnd:
		return l && r
	case ExprOr:
		return l || r
	default:
	}

	return false
}
