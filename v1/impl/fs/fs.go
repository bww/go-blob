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
	"github.com/bww/go-util/v1/contexts"
	"github.com/bww/go-util/v1/urls"

	siter "github.com/bww/go-iterator/v1"
)

const pagelen = 64

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
	r, err := os.Open(p)
	if err != nil && os.IsNotExist(err) {
		return nil, blob.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return r, nil
}

func (c *Client) List(cxt context.Context, rc string, opts ...blob.ReadOption) (siter.Iterator[blob.Resource], error) {
	p, err := c.path(rc)
	if err != nil {
		return nil, err
	}
	if c.log != nil {
		c.log.Info("list", "rc", rc, "root", c.root)
	}

	r, err := os.Open(p)
	if err != nil && os.IsNotExist(err) {
		return nil, blob.ErrNotFound
	} else if err != nil {
		return nil, err
	}

	v, err := r.Stat()
	if err != nil {
		return nil, err
	}
	if !v.IsDir() { // short circut for single-element result
		return siter.NewWithSlice(cxt, []blob.Resource{{
			URL: rc,
		}}), nil
	}

	iter := siter.NewWithContext(cxt, make(chan siter.Result[blob.Resource], pagelen))
	go func() {
		defer iter.Close()
		err := c.list(cxt, rc, p, iter, r)
		if err != nil {
			iter.Cancel(err)
			return
		}
	}()

	return iter, nil
}

func (c *Client) list(cxt context.Context, rc, prefix string, iter siter.Writer[blob.Resource], f *os.File) error {
	dirs, err := f.ReadDir(pagelen)
	if err == io.EOF {
		return nil // end of input
	} else if err != nil {
		return err
	}
	for _, dir := range dirs {
		if !contexts.Continue(cxt) {
			break // canceled
		}
		name := dir.Name()
		if strings.HasPrefix(name, ".") {
			continue // skip dotfiles
		}
		if dir.IsDir() {
			p := path.Join(prefix, name)
			d, err := os.Open(p)
			if err != nil {
				return err
			}
			err = c.list(cxt, urls.Join(rc, name), path.Join(prefix, name), iter, d)
			if err != nil {
				return err
			}
		} else {
			err = iter.Write(blob.Resource{
				URL: urls.Join(rc, name),
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
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

	d := path.Dir(p)
	_, err = os.Stat(d)
	if os.IsNotExist(err) {
		err = os.MkdirAll(d, 0750)
	}
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
