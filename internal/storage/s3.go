package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3Storage implements the Storage interface for AWS S3. It uses the
// AWS SDK v2 for high-performance concurrent uploads and downloads.
type S3Storage struct {
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	prefix     string
}

// NewS3Storage initializes a new S3Storage backend. It loads default AWS
// credentials and configuration from the environment and SDK defaults.
func NewS3Storage(ctx context.Context, bucket, prefix, region string) (*S3Storage, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	return &S3Storage{
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
		bucket:     bucket,
		prefix:     prefix,
	}, nil
}

// fullPath combines the configured prefix with the logical object path.
func (s *S3Storage) fullPath(path string) string {
	return s.prefix + path
}

// Put uploads data to S3 using the high-level 'uploader' manager, which
// handles multipart uploads automatically for large files.
func (s *S3Storage) Put(ctx context.Context, path string, reader io.Reader) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullPath(path)),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}
	return nil
}

// Get retrieves an object from S3 and returns a readable stream.
func (s *S3Storage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullPath(path)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get from S3: %w", err)
	}
	return output.Body, nil
}

// List iterates through all S3 objects under the configured prefix using
// pagination to handle buckets with a large number of artifacts.
func (s *S3Storage) List(ctx context.Context, prefix string) ([]string, error) {
	var paths []string
	fullPrefix := s.fullPath(prefix)

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			relPath := strings.TrimPrefix(*obj.Key, s.prefix)
			paths = append(paths, relPath)
		}
	}

	return paths, nil
}

// Delete removes an object from the S3 bucket.
func (s *S3Storage) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullPath(path)),
	})
	if err != nil {
		return fmt.Errorf("failed to delete from S3: %w", err)
	}
	return nil
}

// Exists checks if an object exists in S3 by performing a lightweight HEAD request.
func (s *S3Storage) Exists(ctx context.Context, path string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullPath(path)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if ok := errors.As(err, &apiErr); ok {
			switch apiErr.ErrorCode() {
			case "NotFound", "NoSuchKey":
				return false, nil
			case "Forbidden", "AccessDenied":
				return false, fmt.Errorf("access denied to S3: %w", err)
			}
		}
		return false, fmt.Errorf("failed to check if S3 object exists: %w", err)
	}
	return true, nil
}
