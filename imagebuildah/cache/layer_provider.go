package cache

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// LayerProvider is an interface for loading and storing layers into a cache system
type LayerProvider interface {
	// Load downloads installs intermittent image, based on key, and returns a cached layer id, based on layer key
	Load(context.Context, string) (string, error)
	// Store the intermittent image into distributed cache
	Store(context.Context, string, string) error
	// PopulateLayer allows for the distributed cache to populate information if required
	PopulateLayer(context.Context, string) error
}

// CalculateBuildLayerKey calculates the key for a layer that is about to be build
func CalculateBuildLayerKey(manifestType string, buildAddsLayer bool, parentLayerID string, nextCreatedBy string, history []v1.History, digests []digest.Digest) string {
	h := sha256.New()

	fmt.Fprintln(h, manifestType)
	fmt.Fprintf(h, "%t\n", buildAddsLayer)
	fmt.Fprintln(h, parentLayerID)

	digestCount := 0
	for _, element := range history {
		fmt.Fprintln(h, element.Created.UTC().String())
		fmt.Fprintln(h, element.CreatedBy)
		fmt.Fprintln(h, element.Author)
		fmt.Fprintln(h, element.Comment)
		fmt.Fprintf(h, "%t\n", element.EmptyLayer)
		fmt.Fprintln(h)
	}

	fmt.Fprintln(h, nextCreatedBy)

	for i := 0; i < digestCount; i++ {
		fmt.Fprintln(h, digests[i].String())
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}
