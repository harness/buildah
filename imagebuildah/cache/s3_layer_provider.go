package cache

import (
	"context"
	"github.com/containers/buildah/imagebuildah/cache/file_transport"
	"github.com/containers/buildah/util"
	"github.com/containers/image/v5/copy"
	is "github.com/containers/image/v5/storage"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

// S3LayerProvider is an implementation of the LayerProvider that
type S3LayerProvider struct {
	store         storage.Store
	systemContext *types.SystemContext
	location      string
}

// NewS3LayerProvider creates a new instance of the CascadeLayerProvider.
func NewS3LayerProvider(store storage.Store, systemContext *types.SystemContext, location string) LayerProvider {
	return &S3LayerProvider{
		store:         store,
		systemContext: systemContext,
		location:      location,
	}
}


// PopulateLayer scans the local images and adds them to the map.
func (slp *S3LayerProvider) PopulateLayer(ctx context.Context, topLayer string) error {
	return nil
}

// Load returns the image id for the key.
func (slp *S3LayerProvider) Load(ctx context.Context, layerKey string) (string, error) {
	return "", nil
}

// Store returns the image id for the key.
func (slp *S3LayerProvider) Store(ctx context.Context, layerKey string, imageID string) error {
	os.MkdirAll(slp.location, 0644)

	srcRef, err := is.Transport.ParseStoreReference(slp.store, "@"+imageID)
	if err != nil {
		return errors.Wrapf(err, "failed to obtain the image reference %q", imageID)
	}

	policyContext, err := util.GetPolicyContext(slp.systemContext)
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

	dir := slp.keyDirectory(layerKey)

	destRef, err := file_transport.NewReference(dir) // should be for s3 call s3 api to store
	if err != nil {
		return err
	}

	_, err = copy.Image(ctx, policyContext, destRef, srcRef, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to store image %q", imageID)
	}

	f, err := os.Create(slp.imageIDFilepath(layerKey))
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

func (slp *S3LayerProvider) keyDirectory(layerKey string) string {
	return filepath.Join(slp.location, layerKey)
}

func (slp *S3LayerProvider) imageIDFilepath(layerKey string) string {
	return filepath.Join(slp.location, layerKey, imageIDFilename)
}