package db

import (
	"strconv"
	"strings"
)

func placeholdersForValues(index int, numberOfArg int) string {
	var buff strings.Builder
	buff.Grow(3 + 3 * numberOfArg)
	buff.WriteByte('(')
	for i := 1; i <= numberOfArg; i++ {
		buff.WriteString(`$`)
		buff.WriteString(strconv.Itoa(index*numberOfArg + i))
		if i < numberOfArg {
			buff.WriteByte(',')
		}
	}
	buff.WriteByte(')')
	return buff.String()
}
