package databath

import (
	"regexp"
)

var re_notAlphaNumeric *regexp.Regexp
var re_numeric *regexp.Regexp
var re_questionmark *regexp.Regexp
var re_fieldInSquares *regexp.Regexp

func init() {
	re_notAlphaNumeric = regexp.MustCompile(`[^a-zA-Z0-9]`)
	re_numeric = regexp.MustCompile(`^[0-9]*$`)
	re_questionmark = regexp.MustCompile(`\?`)
	re_fieldInSquares = regexp.MustCompile(`\[[a-zA-Z0-9_\.]*\]`)
}
