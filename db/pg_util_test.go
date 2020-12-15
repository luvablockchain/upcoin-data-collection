package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPlaceholdersForValues(t *testing.T) {
	test := assert.New(t)

	cases := []struct {
		expected string
		actual string
	} {
		{
			expected: "($1)",
			actual:   placeholdersForValues(0, 1),
		},
		{
			expected: "($1,$2)",
			actual:   placeholdersForValues(0, 2),
		},
		{
			expected: "($4,$5,$6)",
			actual:   placeholdersForValues(1, 3),
		},
		{
			expected: "($1,$2,$3,$4,$5,$6,$7)",
			actual:   placeholdersForValues(0, 7),
		},
		{
			expected: "($11,$12,$13,$14,$15)",
			actual:   placeholdersForValues(2, 5),
		},
	}
	for _, c := range cases {
		test.Equal(c.expected, c.actual)
	}
}
