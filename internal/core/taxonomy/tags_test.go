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

func TestExistingIgnoresRunNewTagsAndDoesNotMutate(t *testing.T) {
	v := NewRunVocab([]string{"LLM"}, map[string]string{"k8s": "go"}, true)
	if got, ok := v.Existing("llms"); !ok || got != "LLM" {
		t.Errorf("Existing(llms) = %q, %v", got, ok)
	}
	if _, ok := v.Existing("brand-new"); ok {
		t.Error("unknown tag reported as existing")
	}
	if len(v.ProposedCounts()) != 0 {
		t.Errorf("Existing mutated the vocab: %v", v.ProposedCounts())
	}
	if _, isNew, ok := v.Resolve("brand-new"); !isNew || !ok {
		t.Fatal("Resolve should accept a new tag")
	}
	if _, ok := v.Existing("brand-new"); ok {
		t.Error("a tag new this run must not count as existing")
	}
}

func TestTermsKeepsOrder(t *testing.T) {
	v := NewRunVocab([]string{"zeta", "alpha", "Alpha"}, nil, true)
	v.Resolve("mid")
	got := v.Terms()
	want := []string{"zeta", "alpha", "mid"}
	if len(got) != len(want) {
		t.Fatalf("Terms() = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Terms() = %v; want %v", got, want)
		}
	}
}
