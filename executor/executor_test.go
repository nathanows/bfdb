package executor_test

import (
	"reflect"
	"testing"

	"github.com/nathanows/bfdb/executor"

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
		expectedResult     []executor.Tuple
		queryTree          executor.PlanNode
	}{
		{
			queryEqv:       `SELECT * FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   allAttrs,
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
			},
		},
		{
			queryEqv:       `SELECT * FROM movies LIMIT 3`,
			expectedTuples: 3,
			expectedCols:   allAttrs,
			queryTree: &executor.LimitNode{
				Limit: 3,
				Child: &executor.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:       `SELECT * FROM movies WHERE id = 3`,
			expectedTuples: 1,
			expectedCols:   allAttrs,
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
				Expr: &executor.Expression{
					Qual: &executor.Qualifier{
						Field: "id",
						Type:  executor.QualEql,
						Value: 3,
					},
				},
			},
		},
		{
			queryEqv:       `SELECT * FROM movies WHERE genre = "sci-fi" LIMIT 2`,
			expectedTuples: 2,
			expectedCols:   allAttrs,
			queryTree: &executor.LimitNode{
				Limit: 2,
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Expr: &executor.Expression{
						Qual: &executor.Qualifier{
							Field: "genre",
							Type:  executor.QualEql,
							Value: "sci-fi",
						},
					},
				},
			},
		},
		{
			queryEqv:           `SELECT * FROM movies ORDER BY name`,
			expectedTuples:     len(inMemMovies),
			expectedCols:       allAttrs,
			expectedResIDOrder: []int{6, 1, 5, 3, 2, 4},
			queryTree: &executor.SortNode{
				Field: "name",
				Dir:   executor.SortAsc,
				Child: &executor.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:           `SELECT * FROM movies ORDER BY name desc`,
			expectedTuples:     len(inMemMovies),
			expectedCols:       allAttrs,
			expectedResIDOrder: []int{4, 2, 3, 5, 1, 6},
			queryTree: &executor.SortNode{
				Field: "name",
				Dir:   executor.SortDesc,
				Child: &executor.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:           `SELECT * FROM movies WHERE genre = "sci-fi" ORDER BY name LIMIT 2`,
			expectedTuples:     2,
			expectedCols:       allAttrs,
			expectedResIDOrder: []int{6, 3},
			queryTree: &executor.LimitNode{
				Limit: 2,
				Child: &executor.SortNode{
					Field: "name",
					Dir:   executor.SortAsc,
					Child: &executor.MemScanNode{
						Src: inMemMovies,
						Expr: &executor.Expression{
							Qual: &executor.Qualifier{
								Field: "genre",
								Type:  executor.QualEql,
								Value: "sci-fi",
							},
						},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT name FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   []string{"name"},
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
				Proj: executor.Projection{
					{"name", ""},
				},
			},
		},
		{
			queryEqv:       `SELECT id, name FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   []string{"id", "name"},
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
				Proj: executor.Projection{
					{"id", ""},
					{"name", ""},
				},
			},
		},
		{
			queryEqv:       `SELECT name AS movie_name FROM movies`,
			expectedTuples: len(inMemMovies),
			expectedCols:   []string{"movie_name"},
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
				Proj: executor.Projection{
					{"name", "movie_name"},
				},
			},
		},
		{
			queryEqv:       `SELECT sum(avg_rating) FROM movies`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"sum(avg_rating)": 22},
			},
			queryTree: &executor.AggNode{
				Type:  executor.AggSum,
				Field: "avg_rating",
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Proj: executor.Projection{
						{"avg_rating", ""},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT sum(avg_rating) AS total FROM movies`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"total": 22},
			},
			queryTree: &executor.AggNode{
				Type:  executor.AggSum,
				Field: "avg_rating",
				Proj: executor.Projection{
					{"sum(avg_rating)", "total"},
				},
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Proj: executor.Projection{
						{"avg_rating", ""},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT sum(avg_rating_f) FROM movies`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"sum(avg_rating_f)": 21.5},
			},
			queryTree: &executor.AggNode{
				Type:  executor.AggSum,
				Field: "avg_rating_f",
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Proj: executor.Projection{
						{"avg_rating_f", ""},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT count(id) FROM movies`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"count(id)": len(inMemMovies)},
			},
			queryTree: &executor.AggNode{
				Type:  executor.AggCount,
				Field: "id",
				Child: &executor.MemScanNode{
					Src: inMemMovies,
				},
			},
		},
		{
			queryEqv:       `SELECT count(id) FROM movies WHERE genre = 'sci-fi'`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"count(id)": 3},
			},
			queryTree: &executor.AggNode{
				Type:  executor.AggCount,
				Field: "id",
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Expr: &executor.Expression{
						Qual: &executor.Qualifier{
							Field: "genre",
							Type:  executor.QualEql,
							Value: "sci-fi",
						},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT name FROM movies WHERE genre = 'sci-fi' AND id = 6`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"name": "Alien"},
			},
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
				Proj: executor.Projection{
					{"name", ""},
				},
				Expr: &executor.Expression{
					Type: executor.ExprAnd,
					Left: &executor.Expression{
						Qual: &executor.Qualifier{
							Field: "genre",
							Type:  executor.QualEql,
							Value: "sci-fi",
						},
					},
					Right: &executor.Expression{
						Qual: &executor.Qualifier{
							Field: "id",
							Type:  executor.QualEql,
							Value: 6,
						},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT count(id) FROM movies WHERE genre = 'sci-fi' OR genre = 'western'`,
			expectedTuples: 1,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"count(id)": 4},
			},
			queryTree: &executor.AggNode{
				Type:  executor.AggCount,
				Field: "id",
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Expr: &executor.Expression{
						Type: executor.ExprOr,
						Left: &executor.Expression{
							Qual: &executor.Qualifier{
								Field: "genre",
								Type:  executor.QualEql,
								Value: "sci-fi",
							},
						},
						Right: &executor.Expression{
							Qual: &executor.Qualifier{
								Field: "genre",
								Type:  executor.QualEql,
								Value: "western",
							},
						},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT name FROM movies WHERE id = 1 OR (genre = 'sci-fi' AND id = 4)`,
			expectedTuples: 2,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"name": "Cool Hand Luke"},
				map[string]interface{}{"name": "Star Wars"},
			},
			queryTree: &executor.MemScanNode{
				Src: inMemMovies,
				Proj: executor.Projection{
					{"name", ""},
				},
				Expr: &executor.Expression{
					Type: executor.ExprOr,
					Left: &executor.Expression{
						Qual: &executor.Qualifier{
							Field: "id",
							Type:  executor.QualEql,
							Value: 1,
						},
					},
					Right: &executor.Expression{
						Type: executor.ExprAnd,
						Left: &executor.Expression{
							Qual: &executor.Qualifier{
								Field: "genre",
								Type:  executor.QualEql,
								Value: "sci-fi",
							},
						},
						Right: &executor.Expression{
							Qual: &executor.Qualifier{
								Field: "id",
								Type:  executor.QualEql,
								Value: 4,
							},
						},
					},
				},
			},
		},
		{
			queryEqv:       `SELECT distinct genre FROM movies`,
			expectedTuples: 4,
			expectedResult: []executor.Tuple{
				map[string]interface{}{"genre": "western"},
				map[string]interface{}{"genre": "animated"},
				map[string]interface{}{"genre": "sci-fi"},
				map[string]interface{}{"genre": "drama"},
			},
			queryTree: &executor.UniqNode{
				Field: "genre",
				Child: &executor.MemScanNode{
					Src: inMemMovies,
					Proj: executor.Projection{
						{"genre", ""},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.queryEqv, func(t *testing.T) {
			resultCh := make(chan executor.Tuple)
			queryDesc := executor.QueryDesc{
				PlanHead: tt.queryTree,
				Dest:     resultCh,
			}

			go executor.Run(queryDesc)

			results := []executor.Tuple{}
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

func pluckIDs(ts []executor.Tuple) []int {
	ids := []int{}
	for _, t := range ts {
		ids = append(ids, t["id"].(int))
	}
	return ids
}
