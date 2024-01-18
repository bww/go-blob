package impl

import (
	"context"
	"fmt"
	"net/url"

	"github.com/bww/go-blob/v1"
	"github.com/bww/go-blob/v1/impl/fs"
	"github.com/bww/go-blob/v1/impl/gcs"
)

// New creates a blob service for the specified DSN. If no such backend is
// supported, an error is returned. Currently, the following backends are
// supported:
//
// - `file://<root>` The local filesystem
// - `gcs://bucket` Google Cloud Storage
func New(cxt context.Context, dsn string) (blob.Service, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case fs.Scheme:
		return fs.New(cxt, dsn)
	case gcs.Scheme:
		return gcs.New(cxt, dsn)
	default:
		return nil, fmt.Errorf("%w: %s", blob.ErrNotSupported, dsn)
	}
}

// NewForResource creates a blob service using the appropriate implementation
// for the provided resource URL. If no such service is supported, an error is
// returned.
//
// After the client is created, the resource URL may be used as input to
// perform operations on it.
func NewForResource(cxt context.Context, rc string) (blob.Service, error) {
	return New(cxt, rc)
}
