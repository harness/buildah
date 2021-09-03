package s3

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Store provides a log storage driver backed by S3 or a
// S3 compatible store.
type Store struct {
	bucket  string
	prefix  string
	acl     string
	session *session.Session
}

// NewEnv returns a new S3 log store from the environment.
func NewEnv(bucket, prefix, endpoint string, pathStyle bool, accessKeyID, accessSecretKey, region, acl string) *Store {
	disableSSL := false

	if endpoint != "" {
		disableSSL = !strings.HasPrefix(endpoint, "https://")
	}

	return &Store{
		bucket: bucket,
		prefix: prefix,
		acl:    acl,
		session: session.Must(
			session.NewSession(&aws.Config{
				Region:           aws.String(region),
				Endpoint:         aws.String(endpoint),
				DisableSSL:       aws.Bool(disableSSL),
				S3ForcePathStyle: aws.Bool(pathStyle),
				Credentials:      credentials.NewStaticCredentials(accessKeyID, accessSecretKey, ""),
			}),
		),
	}
}

// New returns a new S3 log store.
func New(session *session.Session, bucket, prefix string) *Store {
	return &Store{
		bucket:  bucket,
		prefix:  prefix,
		session: session,
	}
}

// Download downloads a log stream from the S3 datastore.
func (s *Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	svc := s3.New(s.session)
	keyWithPrefix := path.Join("/", s.prefix, key)
	out, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(keyWithPrefix),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// Upload uploads the log stream from Reader r to the
// S3 datastore.
func (s *Store) Upload(ctx context.Context, key string, r io.Reader) error {
	uploader := s3manager.NewUploader(s.session)
	keyWithPrefix := path.Join("/", s.prefix, key)
	input := &s3manager.UploadInput{
		ACL:    aws.String(s.acl),
		Bucket: aws.String(s.bucket),
		Key:    aws.String(keyWithPrefix),
		Body:   r,
	}
	_, err := uploader.Upload(input)
	return err
}

// Delete purges the log stream from the S3 datastore.
func (s *Store) Delete(ctx context.Context, key string) error {
	svc := s3.New(s.session)
	keyWithPrefix := path.Join("/", s.prefix, key)
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(keyWithPrefix),
	})
	return err
}
