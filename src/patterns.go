package patterns

import (
	"unicode"
)

// TODO: bench all this

// TODO: doc
func patternise(s string) []byte {
	runes := []rune(s)
	p := make([]byte, 0, len(runes)) // TODO: create a version with a preallocated slice

	var ctype byte
	for _, r := range runes {
		if unicode.IsLetter(r) {
			ctype = 'w'
		} else if unicode.IsDigit(r) {
			ctype = 'd'
		} else if unicode.IsSpace(r) {
			ctype = 's'
		} else {
			ctype = 'x' // TODO: more types?
		}

		p = append(p, ctype)
	}
	return p
}

// TODO: doc
func Patternise(s string) string {
	return string(patternise(s))
}

// TODO: doc
func PatterniseCompact(s string) string {
	pattern := patternise(s)
	if len(pattern) < 2 {
		return string(pattern)
	}
	pos := 1
	for j, b := range pattern[1:] {
		if pattern[j] != b {
			pattern[pos] = b
			pos++
		}
	}

	return string(pattern[:pos])
}

