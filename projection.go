package qexec

type Projection []ProjectionCol

type ProjectionCol struct {
	Name  string
	Alias string
}

func (p Projection) Exec(t Tuple) Tuple {
	if len(p) == 0 {
		return t
	}

	res := map[string]interface{}{}
	for _, c := range p {
		if len(c.Alias) > 0 {
			res[c.Alias] = t[c.Name]
		} else {
			res[c.Name] = t[c.Name]
		}
	}

	return res
}
