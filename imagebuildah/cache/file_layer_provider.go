package cache

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/containers/buildah/imagebuildah/cache/cache_transport"
	"github.com/containers/buildah/util"
	"github.com/containers/image/v5/copy"
	is "github.com/containers/image/v5/storage"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage"
	"github.com/pkg/errors"
)

const imageIDFilename = "imageID"

// FileLayerProvider is an implementation of the LayerProvider that
type FileLayerProvider struct {
	store         storage.Store
	systemContext *types.SystemContext
	location      string
}

// NewFileLayerProvider creates a new instance of the FileLayerProvider.
func NewFileLayerProvider(store storage.Store, systemContext *types.SystemContext, location string) LayerProvider {
	return &FileLayerProvider{
		store:         store,
		systemContext: systemContext,
		location:      location,
	}
}

// PopulateLayer scans the local images and adds them to the map.
func (flp *FileLayerProvider) PopulateLayer(ctx context.Context, topLayer string) error {
	return nil
}

func (flp *FileLayerProvider) keyDirectory(layerKey string) string {
	return filepath.Join(flp.location, layerKey)
}

func (flp *FileLayerProvider) imageIDFilepath(layerKey string) string {
	return filepath.Join(flp.location, layerKey, imageIDFilename)
}

// Load returns the image id for the layerKey.
func (flp *FileLayerProvider) Load(ctx context.Context, layerKey string) (string, error) {
	dir := flp.keyDirectory(layerKey)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// image with that key is not in the cache
		return "", nil
	}

	srcRef, err := cache_transport.NewReference(dir, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create reference for %s", layerKey)
	}

	policyContext, err := util.GetPolicyContext(flp.systemContext)
	if err != nil {
		return "", err
	}
	defer func() {
		if destroyErr := policyContext.Destroy(); destroyErr != nil {
			if err == nil {
				err = destroyErr
			} else {
				err = errors.Wrap(err, destroyErr.Error())
			}
		}
	}()

	imageIDBytes, err := ioutil.ReadFile(flp.imageIDFilepath(layerKey))

	imageID := string(imageIDBytes)

	destRef, err := is.Transport.ParseStoreReference(flp.store, "@"+imageID)
	if err != nil {
		return "", err
	}

	_, err = copy.Image(ctx, policyContext, destRef, srcRef, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to obtain the image for %s", layerKey)
	}

	return imageID, nil
}

// Store stores the image with imageID in the location.
func (flp *FileLayerProvider) Store(ctx context.Context, layerKey string, imageID string) error {
	os.MkdirAll(flp.location, 0644)

	srcRef, err := is.Transport.ParseStoreReference(flp.store, "@"+imageID)
	if err != nil {
		return errors.Wrapf(err, "failed to obtain the image reference %q", imageID)
	}

	policyContext, err := util.GetPolicyContext(flp.systemContext)
	if err != nil {
		return err
	}
	defer func() {
		if destroyErr := policyContext.Destroy(); destroyErr != nil {
			if err == nil {
				err = destroyErr
			} else {
				err = errors.Wrap(err, destroyErr.Error())
			}
		}
	}()

	dir := flp.keyDirectory(layerKey)

	destRef, err := cache_transport.NewReference(dir, nil)
	if err != nil {
		return err
	}

	_, err = copy.Image(ctx, policyContext, destRef, srcRef, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to store image %q", imageID)
	}

	f, err := os.Create(flp.imageIDFilepath(layerKey))
	if err != nil {
		return errors.Wrapf(err, "failed to store image id file %q", imageID)
	}
	defer f.Close()

	_, err = f.WriteString(imageID)
	if err != nil {
		return errors.Wrapf(err, "failed to store image id file %q", imageID)
	}

	return nil
}
