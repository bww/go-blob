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

type Client struct {
	root string
	log  *slog.Logger
}

func New(cxt context.Context, rc string) (*Client, error) {
	return NewWithConfig(cxt, rc, Config{})
}

func NewWithConfig(cxt context.Context, rc string, conf Config) (*Client, error) {
	u, err := url.Parse(rc)
	if err != nil {
		return nil, err
	}
	return &Client{
		root: u.Path,
		log:  conf.Logger,
	}, nil
}

func (c *Client) path(rc string) (string, error) {
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
	if strings.HasPrefix(p, c.root) {
		return p, nil
	} else {
		return path.Join(c.root, p), nil
	}
}

func (c *Client) Init(cxt context.Context, opts ...blob.WriteOption) error {
	return os.MkdirAll(c.root, 0750)
}

func (c *Client) Read(cxt context.Context, rc string, opts ...blob.ReadOption) (io.ReadCloser, error) {
	p, err := c.path(rc)
	if err != nil {
		return nil, err
	}
	if c.log != nil {
		c.log.Info("read", "rc", rc, "root", c.root)
	}
	return os.Open(p)
}

func (c *Client) Accessor(cxt context.Context, rc string, opts ...blob.ReadOption) (string, error) {
	p, err := c.path(rc)
	if err != nil {
		return "", err
	}
	if c.log != nil {
		c.log.Info("accessor", "rc", rc, "root", c.root)
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

func (c *Client) Write(cxt context.Context, rc string, opts ...blob.WriteOption) (io.WriteCloser, error) {
	p, err := c.path(rc)
	if err != nil {
		return nil, err
	}
	if c.log != nil {
		c.log.Info("write", "rc", rc, "root", c.root)
	}
	return os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

func (c *Client) Delete(cxt context.Context, rc string, opts ...blob.WriteOption) error {
	p, err := c.path(rc)
	if err != nil {
		return err
	}
	if c.log != nil {
		c.log.Info("delete", "rc", rc, "root", c.root)
	}
	return os.Remove(p)
}

func (c *Client) String() string {
	return schemePrefix + c.root
}
