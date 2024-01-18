package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bww/go-blob/v1"
)

const (
	Scheme       = "gcs"
	schemePrefix = "gcs://"
)

var ErrInvalidBucket = errors.New("Invalid bucket")

type Config struct {
	Logger *slog.Logger
}

type Service struct {
	client    *storage.Client
	bucket    *storage.BucketHandle
	log       *slog.Logger
	projectId string
	prefix    string
	fqbp      string // fully-qualified bucket prefix
}

func New(cxt context.Context, rc string) (*Service, error) {
	return NewWithConfig(cxt, rc, Config{})
}

func NewWithConfig(cxt context.Context, rc string, conf Config) (*Service, error) {
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
		log:       conf.Logger,
		projectId: dsn.ProjectId,
		prefix:    dsn.Prefix,
		fqbp:      fmt.Sprintf("%s%s/%s/", schemePrefix, dsn.ProjectId, dsn.Prefix),
	}, nil
}

func (s *Service) path(rc string) (string, error) {
	if !strings.HasPrefix(rc, schemePrefix) {
		return rc, nil // just a path
	}
	if !strings.HasPrefix(rc, s.fqbp) {
		return "", fmt.Errorf("%w: expected prefix %q in %q", ErrInvalidBucket, s.fqbp, rc)
	}
	return rc[len(s.fqbp):], nil
}

func (s *Service) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	rc, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	if s.log != nil {
		s.log.Info("read", "rc", rc)
	}
	return s.bucket.Object(rc).NewReader(cxt)
}

func (s *Service) Accessor(cxt context.Context, rc string, opts ...blob.ReadOption) (string, error) {
	rc, err := s.path(rc)
	if err != nil {
		return "", err
	}
	if s.log != nil {
		s.log.Info("accessor", "rc", rc)
	}
	params := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(15 * time.Minute),
	}
	return s.bucket.SignedURL(rc, params)

}

func (s *Service) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	rc, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	if s.log != nil {
		s.log.Info("write", "rc", rc)
	}
	return s.bucket.Object(rc).NewWriter(cxt), nil
}

func (s *Service) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	rc, err := s.path(rc)
	if err != nil {
		return err
	}
	if s.log != nil {
		s.log.Info("delete", "rc", rc)
	}
	return s.bucket.Object(rc).Delete(cxt)
}

func (s *Service) String() string {
	return s.fqbp
}
