package expr

import (
	"reflect"
	"testing"
)

func TestNewIdentifier(t *testing.T) {
	tests := []struct {
		name string
		want *Identifier
	}{
		{"ahoy", &Identifier{Name: "ahoy"}},
		{"Ahoy", &Identifier{Name: "Ahoy", quoted: true}},
		{"hello world", &Identifier{Name: "hello world", quoted: true}},
	}
	for _, test := range tests {
		if got := NewIdentifier(test.name); !reflect.DeepEqual(got, test.want) {
			t.Errorf("NewIdentifier(\"%v\") = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestIdentifierStringer(t *testing.T) {
	tests := []struct {
		idn      Identifier
		expected string
	}{
		{Identifier{Name: "foo"}, "foo"},
		{Identifier{Name: "Foo", quoted: true}, "\"Foo\""},
		{Identifier{Name: "bar", Namespace: &Identifier{Name: "foo"}}, "foo.bar"},
		{Identifier{Name: "bar", Namespace: &Identifier{Name: "Foo", quoted: true}}, "\"Foo\".bar"},
		{Identifier{Name: "Bar", quoted: true, Namespace: &Identifier{Name: "foo"}}, "foo.\"Bar\""},
		{Identifier{Name: "Bar", quoted: true, Namespace: &Identifier{Name: "Foo", quoted: true}}, "\"Foo\".\"Bar\""},
	}
	for _, test := range tests {
		if test.idn.String() != test.expected {
			t.Errorf("Identifier.String() = %v, want %v", test.idn.String(), test.expected)
		}
	}
}
