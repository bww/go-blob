package fs

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/bww/go-blob/v1"
)

const (
	Scheme       = "file"
	schemePrefix = "file://"
)

type Config struct {
	Logger *slog.Logger
}

type Service struct {
	root string
	log  *slog.Logger
}

func New(cxt context.Context, rc string) (*Service, error) {
	return NewWithConfig(cxt, rc, Config{})
}

func NewWithConfig(cxt context.Context, rc string, conf Config) (*Service, error) {
	u, err := url.Parse(rc)
	if err != nil {
		return nil, err
	}
	return &Service{
		root: u.Path,
		log:  conf.Logger,
	}, nil
}

func (s *Service) path(rc string) (string, error) {
	var p string
	if strings.HasPrefix(rc, schemePrefix) {
		u, err := url.Parse(rc)
		if err != nil {
			return "", err
		}
		p = u.Path
	} else {
		p = rc
	}
	if strings.HasPrefix(p, s.root) {
		return p, nil
	} else {
		return path.Join(s.root, p), nil
	}
}

func (s *Service) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	p, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	if s.log != nil {
		s.log.Info("read", "rc", rc, "root", s.root)
	}
	return os.Open(p)
}

func (s *Service) Accessor(cxt context.Context, rc string, opts ...blob.ReadOption) (string, error) {
	p, err := s.path(rc)
	if err != nil {
		return "", err
	}
	if s.log != nil {
		s.log.Info("accessor", "rc", rc, "root", s.root)
	}
	_, err = os.Stat(p)
	if err != nil {
		return "", err
	}
	return (&url.URL{
		Scheme: "file",
		Path:   p,
	}).String(), nil
}

func (s *Service) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	p, err := s.path(rc)
	if err != nil {
		return nil, err
	}
	if s.log != nil {
		s.log.Info("write", "rc", rc, "root", s.root)
	}
	return os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

func (s *Service) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	p, err := s.path(rc)
	if err != nil {
		return err
	}
	if s.log != nil {
		s.log.Info("delete", "rc", rc, "root", s.root)
	}
	return os.Remove(p)
}

func (s *Service) String() string {
	return schemePrefix + s.root
}
