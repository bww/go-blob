package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/bww/go-blob/v1"
)

const Scheme = "gcs"

var ErrInvalidBucket = errors.New("Invalid bucket")

type Service struct {
	client    *storage.Client
	bucket    *storage.BucketHandle
	projectId string
	prefix    string
}

func New(cxt context.Context, rc string) (*Service, error) {
	dsn, err := ParseDSN(rc)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(cxt, dsn.Options...)
	if err != nil {
		return nil, err
	}
	return &Service{
		client:    client,
		bucket:    client.Bucket(dsn.Prefix),
		projectId: dsn.ProjectId,
		prefix:    dsn.Prefix,
	}, nil
}

func (s *Service) path(rc string) (string, error) {
	const scheme = Scheme + "://"
	if !strings.HasPrefix(rc, scheme) {
		return rc, nil // just a path
	}
	p := fmt.Sprintf("%s%s/%s/", scheme, s.projectId, s.prefix)
	if !strings.HasPrefix(rc, p) {
		return "", fmt.Errorf("%w: expected prefix %q in %q", ErrInvalidBucket, p, rc)
	}
	return rc[len(p):], nil
}

func (s *Service) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	rc, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	return s.bucket.Object(rc).NewReader(cxt)
}

func (s *Service) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	rc, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	return s.bucket.Object(rc).NewWriter(cxt), nil
}

func (s *Service) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	rc, err := s.path(rc)
	if err != nil {
		return err
	}
	return s.bucket.Object(rc).Delete(cxt)
}
