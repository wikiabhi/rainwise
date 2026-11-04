package domain

import (
	"errors"
	"testing"
)

func TestParseID(t *testing.T) {
	ok := []struct {
		in   string
		want ID
	}{
		{"raindrop:123", ID{"raindrop", "123"}},
		{"a:b:c", ID{"a", "b:c"}},
	}
	for _, c := range ok {
		got, err := ParseID(c.in)
		if err != nil || got != c.want {
			t.Errorf("ParseID(%q) = %v, %v; want %v", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", "x", ":1", "x:"} {
		if _, err := ParseID(in); !errors.Is(err, ErrValidation) {
			t.Errorf("ParseID(%q) err = %v; want ErrValidation", in, err)
		}
	}
}

func TestIDString(t *testing.T) {
	for _, s := range []string{"raindrop:12345", "a:b:c"} {
		id, err := ParseID(s)
		if err != nil || id.String() != s {
			t.Errorf("round trip %q = %q, %v", s, id.String(), err)
		}
	}
}
