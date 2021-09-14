package cache

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/containers/buildah/define"
	"github.com/containers/buildah/imagebuildah/cache/file_transport"
	"github.com/containers/buildah/util"
	"github.com/containers/image/v5/copy"
	is "github.com/containers/image/v5/storage"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage"
	"github.com/pkg/errors"
)

// S3LayerProvider is an implementation of the LayerProvider that
type S3LayerProvider struct {
	store          storage.Store
	systemContext  *types.SystemContext
	location       string
	s3CacheOptions *define.S3CacheOptions
}

// NewS3LayerProvider creates a new instance of the CascadeLayerProvider.
func NewS3LayerProvider(store storage.Store, systemContext *types.SystemContext, location string, s3CacheOptions *define.S3CacheOptions) LayerProvider {
	return &S3LayerProvider{
		store:          store,
		systemContext:  systemContext,
		location:       location,
		s3CacheOptions: s3CacheOptions,
	}
}

type Manifest struct {
	SchemaVersion string  `json:"schemaVersion"`
	Config        string  `json:"config"`
	Layers        []Layer `json:"layers"`
}

type Layer struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int    `json:"size"`
}

// PopulateLayer scans the local images and adds them to the map.
func (slp *S3LayerProvider) PopulateLayer(ctx context.Context, topLayer string) error {
	return nil
}

// Load returns the image id for the key.
func (slp *S3LayerProvider) Load(ctx context.Context, layerKey string) (string, error) {
	dir := slp.keyDirectory(layerKey)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// image with that key is not in the cache
		existInS3, err := slp.tryDownloadLayerFromS3(layerKey, dir)
		if !existInS3 || err != nil {
			return "", errors.Wrapf(err, "Local cache does not exist and S3 cache failed to retrieve")
		}
	}

	srcRef, err := file_transport.NewReference(dir, slp.s3CacheOptions)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create reference for %s", layerKey)
	}

	policyContext, err := util.GetPolicyContext(slp.systemContext)
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

	imageIDBytes, err := ioutil.ReadFile(slp.imageIDFilepath(layerKey))
	if err != nil {
		return "", errors.Wrapf(err, "Unable to read file for layer %s", layerKey)
	}

	imageID := string(imageIDBytes)
	destRef, err := is.Transport.ParseStoreReference(slp.store, "@"+imageID)
	if err != nil {
		return "", errors.Wrapf(err, "Unable to parse store reference for layer %s, image id: %s", layerKey, imageID)
	}

	_, err = copy.Image(ctx, policyContext, destRef, srcRef, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to obtain the image for %s", layerKey)
	}

	return imageID, nil
}

func (slp *S3LayerProvider) tryDownloadLayerFromS3(layerKey string, dir string) (bool, error) {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(slp.s3CacheOptions.S3Key, slp.s3CacheOptions.S3Secret, ""),
		Endpoint:         aws.String(slp.s3CacheOptions.S3EndPoint),
		Region:           aws.String(slp.s3CacheOptions.S3Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(s3Config))
	manager := s3manager.NewDownloader(sess)

	d := downloader{bucket: slp.s3CacheOptions.S3Bucket, dir: slp.location, Downloader: manager}
	input := &s3.ListObjectsInput{
		Bucket: aws.String(slp.s3CacheOptions.S3Bucket),
		Prefix: aws.String(layerKey),
	}
	client := s3.New(sess)
	err := client.ListObjectsPages(input, d.eachPage)
	if err != nil {
		return false, err
	}
	content, err := os.ReadFile(path.Join(dir, "imageID"))
	if err != nil {
		// cache does not exist
		return false, nil
	}
	imageId := string(content)
	fpath := path.Join(slp.location, "blobs", imageId)
	if slp.downloadFileIfNotExist(fpath, path.Join("blobs", imageId), manager) != nil {
		return false, err
	}
	byteValue, err := ioutil.ReadFile(path.Join(dir, "manifest.json"))
	if err != nil {
		return false, errors.Wrapf(err, "Failed to read manifest from downloaded cache for layer %s", layerKey)
	}
	var manifest Manifest
	err = json.Unmarshal(byteValue, &manifest)
	if err != nil {
		return false, errors.Wrap(err, "Manifest is not a valid Json")
	}
	for _, layer := range manifest.Layers {
		if len(layer.Digest) < 7 {
			return false, errors.Wrapf(err, "Failed to read manifest from downloaded cache for layer %s", layerKey)
		}
		imageId = layer.Digest[7:]
		fpath := path.Join(slp.location, "blobs", imageId)
		if slp.downloadFileIfNotExist(fpath, path.Join("blobs", imageId), manager) != nil {
			return false, err
		}
	}
	return true, nil
}

func (slp *S3LayerProvider) downloadFileIfNotExist(fpath string, s3path string, manager *s3manager.Downloader) error {
	if _, err := os.Stat(fpath); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(fpath), 0775); err != nil {
			return errors.Wrapf(err, "Unable to Access %s", fpath)
		}
		file, err := os.Create(fpath)
		if err != nil {
			return errors.Wrapf(err, "Unable to Create file at %s", fpath)
		}
		defer file.Close()
		getLayerInput := &s3.GetObjectInput{
			Bucket: aws.String(slp.s3CacheOptions.S3Bucket),
			Key:    aws.String(path.Join(s3path)),
		}
		numBytes, err := manager.Download(file, getLayerInput)
		// the layer might not exist
		if err != nil {
			return errors.Wrapf(err, "Cache layer download failed")
		}
		if numBytes == 0 {
			return errors.Errorf("Downloaded cache is empty")
		}
	}
	return nil
}

type downloader struct {
	*s3manager.Downloader
	bucket, dir string
}

func (d *downloader) eachPage(page *s3.ListObjectsOutput, more bool) bool {
	for _, obj := range page.Contents {
		d.downloadToFile(*obj.Key)
	}

	return true
}

func (d *downloader) downloadToFile(key string) {
	file := filepath.Join(d.dir, key)
	if err := os.MkdirAll(filepath.Dir(file), 0775); err != nil {
		return
	}

	fd, err := os.Create(file)
	if err != nil {
		return
	}
	defer fd.Close()

	params := &s3.GetObjectInput{Bucket: &d.bucket, Key: &key}
	d.Download(fd, params)
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

	destRef, err := file_transport.NewReference(dir, slp.s3CacheOptions)
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

	file, _ := os.ReadDir(dir)
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(slp.s3CacheOptions.S3Key, slp.s3CacheOptions.S3Secret, ""),
		Endpoint:         aws.String(slp.s3CacheOptions.S3EndPoint),
		Region:           aws.String(slp.s3CacheOptions.S3Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(s3Config))
	uploader := s3manager.NewUploader(sess)
	for _, s := range file {
		blob, _ := os.Open(path.Join(dir, s.Name()))
		input := &s3manager.UploadInput{
			Bucket: aws.String(slp.s3CacheOptions.S3Bucket),
			Key:    aws.String(path.Join(layerKey, s.Name())),
			Body:   blob,
		}
		_, err = uploader.Upload(input)
		if err != nil {
			return errors.Wrapf(err, "failed to upload image id to S3 %q", imageID)
		}
	}

	return nil
}

func (slp *S3LayerProvider) keyDirectory(layerKey string) string {
	return filepath.Join(slp.location, layerKey)
}

func (slp *S3LayerProvider) imageIDFilepath(layerKey string) string {
	return filepath.Join(slp.location, layerKey, imageIDFilename)
}
