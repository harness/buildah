package cache

import (
	"context"
)

// CascadeLayerProvider is an implementation of the LayerProvider that
type CascadeLayerProvider struct {
	layerProviders []LayerProvider
}

// NewCascadeLayerProvider creates a new instance of the CascadeLayerProvider.
func NewCascadeLayerProvider(layerProviders []LayerProvider) LayerProvider {
	return &CascadeLayerProvider{
		layerProviders: layerProviders,
	}
}

// PopulateLayer scans the local images and adds them to the map.
func (clp *CascadeLayerProvider) PopulateLayer(ctx context.Context, topLayer string) error {
	for _, layerProvider := range clp.layerProviders {
		err := layerProvider.PopulateLayer(ctx, topLayer)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load returns the image id for the key.
func (clp *CascadeLayerProvider) Load(ctx context.Context, layerKey string) (string, error) {
	for _, layerProvider := range clp.layerProviders {
		imageID, err := layerProvider.Load(ctx, layerKey)
		if err != nil {
			return "", err
		}
		if imageID != "" {
			return imageID, nil
		}
	}
	return "", nil
}

// Store returns the image id for the key.
func (clp *CascadeLayerProvider) Store(ctx context.Context, layerKey string, value string) error {
	for _, layerProvider := range clp.layerProviders {
		err := layerProvider.Store(ctx, layerKey, value)
		if err != nil {
			return err
		}
	}
	return nil
}
