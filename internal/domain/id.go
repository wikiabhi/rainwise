package domain

import (
	"fmt"
	"strings"
)

// ID is provider-qualified, written as "provider:local".
type ID struct {
	Provider string `json:"provider" yaml:"provider"`
	Local    string `json:"local" yaml:"local"`
}

// ParseID parses "provider:local". The local part may itself contain colons.
func ParseID(s string) (ID, error) {
	p, l, ok := strings.Cut(s, ":")
	if !ok || p == "" || l == "" {
		return ID{}, fmt.Errorf("invalid id %q: %w", s, ErrValidation)
	}
	return ID{Provider: p, Local: l}, nil
}

func (id ID) String() string { return id.Provider + ":" + id.Local }

// IsZero reports whether the ID is unset.
func (id ID) IsZero() bool { return id == ID{} }
