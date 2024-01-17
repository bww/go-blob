package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/bww/go-blob/v1"
)

type Service struct {
	client *storage.Client
	prefix string
}

func New(cxt context.Context, rc string) (*Service, error) {
	dsn, err := ParseDSN(rc)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(cxt, dsn.Options...)
	return &Service{
		client: client,
		prefix: dsn.Prefix,
	}, nil
}

func (s *Service) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	return nil, blob.ErrNotSupported
}

func (s *Service) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	return nil, blob.ErrNotSupported
}

func (s *Service) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	return blob.ErrNotSupported
}
