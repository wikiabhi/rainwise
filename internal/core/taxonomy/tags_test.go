package taxonomy

import "testing"

func TestNormalize(t *testing.T) {
	aliases := map[string]string{"k8s": "kubernetes"}
	cases := []struct{ in, want string }{
		{"#Postgres", "postgres"},
		{"  System Design ", "system-design"},
		{"K8s", "kubernetes"},
		{"#", ""},
	}
	for _, c := range cases {
		if got := Normalize(c.in, aliases); got != c.want {
			t.Errorf("Normalize(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestComparisonKey(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Databases", "database"}, {"llms", "llm"}, {"class", "class"},
		{"bonus", "bonus"}, {"analysis", "analysis"}, {"gos", "gos"}, {"Go", "go"},
	}
	for _, c := range cases {
		if got := ComparisonKey(c.in); got != c.want {
			t.Errorf("ComparisonKey(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func TestRunVocabResolve(t *testing.T) {
	v := NewRunVocab([]string{"database", "Go"}, nil, true)
	cases := []struct {
		in    string
		final string
		isNew bool
		ok    bool
	}{
		{"databases", "database", false, true},
		{"go", "Go", false, true},
		{"kubernetes", "kubernetes", true, true},
		{"llms", "llms", true, true},
		{"llm", "llms", false, true},
		{"#", "", false, false},
	}
	for _, c := range cases {
		f, n, ok := v.Resolve(c.in)
		if f != c.final || n != c.isNew || ok != c.ok {
			t.Errorf("Resolve(%q) = %q,%v,%v; want %q,%v,%v", c.in, f, n, ok, c.final, c.isNew, c.ok)
		}
	}
	pc := v.ProposedCounts()
	if pc["kubernetes"] != 1 || pc["llms"] != 2 || len(pc) != 2 {
		t.Errorf("ProposedCounts = %v", pc)
	}

	closed := NewRunVocab([]string{"database"}, nil, false)
	if f, n, ok := closed.Resolve("kubernetes"); f != "" || n || ok {
		t.Errorf("closed vocab Resolve = %q,%v,%v", f, n, ok)
	}
}
