package qexec_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/nathanows/qexec"

	"github.com/stretchr/testify/assert"
)

var inMemMovies = []qexec.Tuple{
	&Movie{1, "Cool Hand Luke", "western"},
	&Movie{2, "Peter Pan", "animated"},
	&Movie{3, "Lord of the Rings", "sci-fi"},
	&Movie{4, "Star Wars", "sci-fi"},
	&Movie{5, "Good Will Hunting", "drama"},
	&Movie{6, "Alien", "sci-fi"},
}

type Movie struct {
	id    int64
	name  string
	genre string
}

func (m *Movie) String() string {
	return fmt.Sprintf("%d,%s,%s", m.id, m.name, m.genre)
}

func TestQueries(t *testing.T) {
	tests := []struct {
		queryEqv           string
		expectedTuples     int
		expectedCols       []string
		expectedResIDOrder []int64 // expected ID set in order, if empty, not checked
		queryTree          qexec.PlanNode
	}{
		{
			queryEqv:       `SELECT * FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   []string{"id", "name", "genre"},
			queryTree: &qexec.MemScanNode{
				Src: inMemMovies,
			},
		},
		{
			queryEqv:       `SELECT * FROM movies LIMIT 3`,
			expectedTuples: 3,
			expectedCols:   []string{"id", "name", "genre"},
			queryTree: &qexec.LimitNode{
				Limit: 3,
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:       `SELECT * FROM movies WHERE id = 3`,
			expectedTuples: 1,
			expectedCols:   []string{"id", "name", "genre"},
			queryTree: &qexec.MemScanNode{
				Src: inMemMovies,
				Qual: &qexec.Qualifier{
					Field: "id",
					Type:  qexec.QualEql,
					Value: "3",
				},
			},
		},
		{
			queryEqv:       `SELECT * FROM movies WHERE genre = "sci-fi" LIMIT 2`,
			expectedTuples: 2,
			expectedCols:   []string{"id", "name", "genre"},
			queryTree: &qexec.LimitNode{
				Limit: 2,
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
					Qual: &qexec.Qualifier{
						Field: "genre",
						Type:  qexec.QualEql,
						Value: "sci-fi",
					},
				},
			},
		},
		{
			queryEqv:           `SELECT * FROM movies ORDER BY name`,
			expectedTuples:     len(inMemMovies),
			expectedCols:       []string{"id", "name", "genre"},
			expectedResIDOrder: []int64{6, 1, 5, 3, 2, 4},
			queryTree: &qexec.SortNode{
				Field: "name",
				Dir:   qexec.SortAsc,
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:           `SELECT * FROM movies ORDER BY name desc`,
			expectedTuples:     len(inMemMovies),
			expectedCols:       []string{"id", "name", "genre"},
			expectedResIDOrder: []int64{4, 2, 3, 5, 1, 6},
			queryTree: &qexec.SortNode{
				Field: "name",
				Dir:   qexec.SortDesc,
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:           `SELECT * FROM movies WHERE genre = "sci-fi" ORDER BY name LIMIT 2`,
			expectedTuples:     2,
			expectedCols:       []string{"id", "name", "genre"},
			expectedResIDOrder: []int64{6, 3},
			queryTree: &qexec.LimitNode{
				Limit: 2,
				Child: &qexec.SortNode{
					Field: "name",
					Dir:   qexec.SortAsc,
					Child: &qexec.MemScanNode{
						Src: inMemMovies,
						Qual: &qexec.Qualifier{
							Field: "genre",
							Type:  qexec.QualEql,
							Value: "sci-fi",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.queryEqv, func(t *testing.T) {
			resultCh := make(chan qexec.Tuple)
			queryDesc := qexec.QueryDesc{
				PlanHead: tt.queryTree,
				Dest:     resultCh,
			}

			go qexec.Run(queryDesc)

			results := []qexec.Tuple{}
			for tup := range resultCh {
				results = append(results, tup)
			}

			assert.Len(t, results, tt.expectedTuples, "wrong # of tuples returned")

			if len(results) > 0 {
				rAttrs := resultAttributes(results[0])
				assert.Len(t, rAttrs, len(tt.expectedCols), "wrong # of attributes returned")

				assert.True(t, reflect.DeepEqual(rAttrs, tt.expectedCols), "wrong attributes returned")

				if len(tt.expectedResIDOrder) > 0 {
					ids := pluckIDs(results)
					assert.True(t, reflect.DeepEqual(ids, tt.expectedResIDOrder), "ids returned in wrong order")
				}
			}
		})
	}
}

func resultAttributes(t qexec.Tuple) []string {
	names := []string{}
	val := reflect.ValueOf(t).Elem()
	for i := 0; i < val.NumField(); i++ {
		names = append(names, val.Type().Field(i).Name)
	}
	return names
}

func pluckIDs(ts []qexec.Tuple) []int64 {
	ids := []int64{}
	for _, t := range ts {
		f := reflect.Indirect(reflect.ValueOf(t)).FieldByName("id")
		ids = append(ids, f.Int())
	}
	return ids
}
