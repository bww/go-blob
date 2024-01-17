package impl

import (
	"context"
	"fmt"
	"net/url"

	"github.com/bww/go-blob/v1"
	"github.com/bww/go-blob/v1/fs"
	"github.com/bww/go-blob/v1/gcs"
)

// New creates a blob service for the specified DSN. If no such backend is
// supported, an error is returned. Currently, the following backends are
// supported:
//
// - `file://<root>` The local filesystem
// - `gcs://bucket` Google Cloud Storage
func New(cxt context.Context, dsn string) (Service, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "fs":
		return fs.New(cxt, dsn)
	case "gcs":
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
func NewForResource(cxt context.Context, rc string) (Service, error) {
	return New(cxt, rc)
}
