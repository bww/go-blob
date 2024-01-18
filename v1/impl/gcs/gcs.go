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
	BucketAttrs *storage.BucketAttrs
	Logger      *slog.Logger
}

type Client struct {
	client    *storage.Client
	bucket    *storage.BucketHandle
	log       *slog.Logger
	projectId string
	prefix    string
	fqbp      string // fully-qualified bucket prefix
	config    Config
}

func New(cxt context.Context, rc string) (*Client, error) {
	return NewWithConfig(cxt, rc, Config{})
}

func NewWithConfig(cxt context.Context, rc string, conf Config) (*Client, error) {
	dsn, err := ParseDSN(rc)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(cxt, dsn.Options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		client:    client,
		bucket:    client.Bucket(dsn.Prefix),
		log:       conf.Logger,
		projectId: dsn.ProjectId,
		prefix:    dsn.Prefix,
		fqbp:      fmt.Sprintf("%s%s/%s/", schemePrefix, dsn.ProjectId, dsn.Prefix),
		config:    conf,
	}, nil
}

func (c *Client) path(rc string) (string, error) {
	if !strings.HasPrefix(rc, schemePrefix) {
		return rc, nil // just a path
	}
	if !strings.HasPrefix(rc, c.fqbp) {
		return "", fmt.Errorf("%w: expected prefix %q in %q", ErrInvalidBucket, c.fqbp, rc)
	}
	return rc[len(c.fqbp):], nil
}

func (c *Client) Init(cxt context.Context, opts ...blob.WriteOption) error {
	_, err := c.bucket.Attrs(cxt)
	if err == nil {
		return nil // already exists
	} else if !errors.Is(err, storage.ErrBucketNotExist) {
		return err // only not-found is acceptable
	}
	var attrs *storage.BucketAttrs
	if c.config.BucketAttrs != nil {
		attrs = c.config.BucketAttrs
	} else {
		attrs = &storage.BucketAttrs{}
	}
	return c.bucket.Create(cxt, c.projectId, attrs)
}

func (c *Client) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	rc, err := c.path(rc)
	if err != nil {
		return nil, err
	}
	if c.log != nil {
		c.log.Info("read", "rc", rc)
	}
	return c.bucket.Object(rc).NewReader(cxt)
}

func (c *Client) Accessor(cxt context.Context, rc string, opts ...blob.ReadOption) (string, error) {
	rc, err := c.path(rc)
	if err != nil {
		return "", err
	}
	if c.log != nil {
		c.log.Info("accessor", "rc", rc)
	}
	params := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(15 * time.Minute),
	}
	return c.bucket.SignedURL(rc, params)

}

func (c *Client) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	rc, err := c.path(rc)
	if err != nil {
		return nil, err
	}
	if c.log != nil {
		c.log.Info("write", "rc", rc)
	}
	return c.bucket.Object(rc).NewWriter(cxt), nil
}

func (c *Client) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	rc, err := c.path(rc)
	if err != nil {
		return err
	}
	if c.log != nil {
		c.log.Info("delete", "rc", rc)
	}
	return c.bucket.Object(rc).Delete(cxt)
}

func (c *Client) String() string {
	return c.fqbp
}
