package memory_test

import (
	"testing"

	"github.com/wikiabhi/rainwise/internal/adapters/store/memory"
	"github.com/wikiabhi/rainwise/internal/adapters/store/storetest"
	"github.com/wikiabhi/rainwise/internal/ports"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ports.Store { return memory.New() })
}
