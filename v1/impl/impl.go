package impl

import (
	"context"
	"fmt"
	"net/url"

	"github.com/bww/go-blob/v1"
	"github.com/bww/go-blob/v1/fs"
	"github.com/bww/go-blob/v1/gcs"
)

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
