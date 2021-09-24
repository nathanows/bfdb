package qexec_test

import (
	"reflect"
	"testing"

	"github.com/nathanows/qexec"

	"github.com/stretchr/testify/assert"
)

var inMemMovies = []interface{}{
	Movie{1, "Cool Hand Luke", "western", 4, 4.25},
	Movie{2, "Peter Pan", "animated", 2, 1.75},
	Movie{3, "Lord of the Rings", "sci-fi", 5, 4.5},
	Movie{4, "Star Wars", "sci-fi", 4, 4.25},
	Movie{5, "Good Will Hunting", "drama", 4, 3.75},
	Movie{6, "Alien", "sci-fi", 3, 3.0},
}

var allAttrs = []string{"id", "name", "genre", "avg_rating", "avg_rating_f"}

type Movie struct {
	ID         int
	Name       string
	Genre      string
	AvgRating  int
	AvgRatingF float64
}

func TestQueries(t *testing.T) {
	tests := []struct {
		queryEqv           string
		expectedTuples     int
		expectedCols       []string
		expectedResIDOrder []int // expected ID set in order, if empty, not checked
		expectedResult     []qexec.Tuple
		queryTree          qexec.PlanNode
	}{
		{
			queryEqv:       `SELECT * FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   allAttrs,
			queryTree: &qexec.MemScanNode{
				Src: inMemMovies,
			},
		},
		{
			queryEqv:       `SELECT * FROM movies LIMIT 3`,
			expectedTuples: 3,
			expectedCols:   allAttrs,
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
			expectedCols:   allAttrs,
			queryTree: &qexec.MemScanNode{
				Src: inMemMovies,
				Qual: &qexec.Qualifier{
					Field: "id",
					Type:  qexec.QualEql,
					Value: 3,
				},
			},
		},
		{
			queryEqv:       `SELECT * FROM movies WHERE genre = "sci-fi" LIMIT 2`,
			expectedTuples: 2,
			expectedCols:   allAttrs,
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
			expectedCols:       allAttrs,
			expectedResIDOrder: []int{6, 1, 5, 3, 2, 4},
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
			expectedCols:       allAttrs,
			expectedResIDOrder: []int{4, 2, 3, 5, 1, 6},
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
			expectedCols:       allAttrs,
			expectedResIDOrder: []int{6, 3},
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
		{
			queryEqv:       `SELECT name FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   []string{"name"},
			queryTree: &qexec.MemScanNode{
				Src: inMemMovies,
				Proj: qexec.Projection{
					{"name", ""},
				},
			},
		},
		{
			queryEqv:       `SELECT name AS movie_name FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   []string{"movie_name"},
			queryTree: &qexec.MemScanNode{
				Src: inMemMovies,
				Proj: qexec.Projection{
					{"name", "movie_name"},
				},
			},
		},
		{
			queryEqv:       `SELECT sum(avg_rating) FROM movies`,
			expectedTuples: 1,
			expectedResult: []qexec.Tuple{
				map[string]interface{}{"sum(avg_rating)": 22},
			},
			queryTree: &qexec.AggNode{
				Type:  qexec.AggSum,
				Field: "avg_rating",
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
					Proj: qexec.Projection{
						{"avg_rating", ""},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT sum(avg_rating) AS total FROM movies`,
			expectedTuples: 1,
			expectedResult: []qexec.Tuple{
				map[string]interface{}{"total": 22},
			},
			queryTree: &qexec.AggNode{
				Type:  qexec.AggSum,
				Field: "avg_rating",
				Proj: qexec.Projection{
					{"sum(avg_rating)", "total"},
				},
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
					Proj: qexec.Projection{
						{"avg_rating", ""},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT sum(avg_rating_f) FROM movies`,
			expectedTuples: 1,
			expectedResult: []qexec.Tuple{
				map[string]interface{}{"sum(avg_rating_f)": 21.5},
			},
			queryTree: &qexec.AggNode{
				Type:  qexec.AggSum,
				Field: "avg_rating_f",
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
					Proj: qexec.Projection{
						{"avg_rating_f", ""},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT count(id) FROM movies`,
			expectedTuples: 1,
			expectedResult: []qexec.Tuple{
				map[string]interface{}{"count(id)": len(inMemMovies)},
			},
			queryTree: &qexec.AggNode{
				Type:  qexec.AggCount,
				Field: "id",
				Child: &qexec.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:       `SELECT count(id) FROM movies WHERE genre = 'sci-fi'`,
			expectedTuples: 1,
			expectedResult: []qexec.Tuple{
				map[string]interface{}{"count(id)": 3},
			},
			queryTree: &qexec.AggNode{
				Type:  qexec.AggCount,
				Field: "id",
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

			if len(tt.expectedCols) > 0 {
				returnedCols := []string{}
				for k := range results[0] {
					returnedCols = append(returnedCols, k)
				}
				assert.ElementsMatch(t, tt.expectedCols, returnedCols, "wrong attributes returned")
			}

			if len(tt.expectedResIDOrder) > 0 {
				ids := pluckIDs(results)
				assert.True(t, reflect.DeepEqual(ids, tt.expectedResIDOrder), "ids returned in wrong order")
			}

			if len(tt.expectedResult) > 0 {
				assert.ElementsMatch(t, tt.expectedResult, results, "incorrect results")
			}
		})
	}
}

func pluckIDs(ts []qexec.Tuple) []int {
	ids := []int{}
	for _, t := range ts {
		ids = append(ids, t["id"].(int))
	}
	return ids
}
