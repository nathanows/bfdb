package executor

import (
	"regexp"
	"strings"
)

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

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
