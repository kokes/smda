package patterns

import "testing"

func TestBasicPatterns(t *testing.T) {
	tt := []struct{ raw, expected string }{
		{"", ""},
		{"John Doe", "wwwwswww"},
		{"John-Doe", "wwwwxwww"},
		{"999-word", "dddxwwww"},
		{"-900", "xddd"},
		{"-2.45", "xdxdd"},
	}
	for _, test := range tt {
		pattern := Patternise(test.raw)
		if pattern != test.expected {
			t.Errorf("Expected %v, got %v", test.expected, pattern)
		}
	}
}

func TestBasicPatternsCompact(t *testing.T) {
	tt := []struct{ raw, expected string }{
		{"", ""},
		{"A", "w"},
		{"AA", "w"},
		{"John Doe", "wsw"},
		{"JohnDoe", "w"},
		{"John-Doe", "wxw"},
		{"999", "d"},
	}
	for _, test := range tt {
		pattern := PatterniseCompact(test.raw)
		if pattern != test.expected {
			t.Errorf("Expected %v, got %v", test.expected, pattern)
		}
	}
}

