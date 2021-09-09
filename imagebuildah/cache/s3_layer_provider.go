package cache

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/containers/buildah/imagebuildah/cache/file_transport"
	"github.com/containers/buildah/util"
	"github.com/containers/image/v5/copy"
	is "github.com/containers/image/v5/storage"
	"github.com/containers/image/v5/types"
	"github.com/containers/storage"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
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
	dir := slp.keyDirectory(layerKey)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// image with that key is not in the cache
		if !slp.getS3Storage(layerKey) {
			return "", nil
		}
	}

	srcRef, err := file_transport.NewReference(dir, false)
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

	imageID := string(imageIDBytes)

	destRef, err := is.Transport.ParseStoreReference(slp.store, "@"+imageID)
	if err != nil {
		return "", err
	}

	_, err = copy.Image(ctx, policyContext, destRef, srcRef, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to obtain the image for %s", layerKey)
	}

	return imageID, nil
}

func (slp *S3LayerProvider) getS3Storage(layerKey string)  bool {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		Endpoint:         aws.String("http://192.168.1.40:9000"),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(s3Config))
	if keyExist(sess, "test3", layerKey) {
		manager := s3manager.NewDownloader(sess)
		d := downloader{bucket: "test3", dir: "", Downloader: manager}
		input := &s3.ListObjectsInput{
			Bucket: 	aws.String("tests3"),
			Prefix:    	aws.String(layerKey),
		}
		client := s3.New(sess)
		err := client.ListObjectsPages(input, d.eachPage)
		if err != nil {
			return false
		}
		return true
	}
	return false

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
	// Create the directories in the path
	file := filepath.Join(d.dir, key)
	if err := os.MkdirAll(filepath.Dir(file), 0775); err != nil {
		panic(err)
	}

	// Set up the local file
	fd, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	// Download the file using the AWS SDK for Go
	fmt.Printf("Downloading s3://%s/%s to %s...\n", d.bucket, key, file)
	params := &s3.GetObjectInput{Bucket: &d.bucket, Key: &key}
	d.Download(fd, params)
}


func keyExist(sess *session.Session, bucket string, key string) bool {
	s3svc := s3.New(sess)
	maxKey := int64(1)
	res, err := s3svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
		MaxKeys: &maxKey,
	})

	if err != nil {
		if int(*res.KeyCount) > 0 {
			return true
		}
	}
	return true
}

// Store returns the image id for the key.
func (slp *S3LayerProvider) Store(ctx context.Context, layerKey string, imageID string) error {
	os.MkdirAll(slp.location, 0644)
	println("S3!!")
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

	destRef, err := file_transport.NewReference(dir, true)
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
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
		Endpoint:         aws.String("http://192.168.1.40:9000"),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(s3Config))
	uploader := s3manager.NewUploader(sess)
	for _, s := range file {
		blob, _ := os.Open(path.Join(dir, s.Name()))
		input := &s3manager.UploadInput{
			Bucket: aws.String("tests3"),
			Key:    aws.String(path.Join(layerKey, s.Name())),
			Body:   blob,
		}
		_, err = uploader.Upload(input)
		if err != nil {
			println(err.Error())
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