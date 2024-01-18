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
func New(cxt context.Context, dsn string) (blob.Client, error) {
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
