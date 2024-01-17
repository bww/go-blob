package fs

import (
	"context"
	"io"
	"net/url"
	"os"
	"path"

	"github.com/bww/go-blob/v1"
)

type Service struct {
	root string
}

func New(cxt context.Context, dsn string) (*Service, error) {
	u, err := url.Parse(rc)
	if err != nil {
		return nil, err
	}
	return &Service{
		root: u.Path,
	}, nil
}

func (s *Service) path(rc string) (string, error) {
	u, err := url.Parse(rc)
	if err != nil {
		return nil, err
	}
	return path.Join(s.root, u.Path), nil
}

func (s *Service) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	p, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

func (s *Service) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	p, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0644)
}

func (s *Service) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	p, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	return os.Remove(p)
}
