package blob

import (
	"context"
	"io"

	siter "github.com/bww/go-iterator/v1"
)

type Resource struct {
	URL         string
	ContentType string
}

type Client interface {
	// Init initializes a blob client in an implementation-specific way; for example, by creating the root path or GCS bucket it uses
	Init(cxt context.Context, opts ...WriteOption) error
	// Read obtains a stream to the specified resource
	Read(cxt context.Context, url string, opts ...ReadOption) (io.ReadCloser, error)
	// List iterates over resources under a prefix URL, producing a description of each one
	List(cxt context.Context, url string, opts ...ReadOption) (siter.Iterator[Resource], error)
	// Accessor obtains a URL which provides access to the underlying resource; for example, a signed GCS URL
	Accessor(cxt context.Context, url string, opts ...ReadOption) (string, error)
	// Write obtains a writer which writes to the specified resource; if it does not exist, it is created; if it does exist it is overwritten
	Write(cxt context.Context, url string, opts ...WriteOption) (io.WriteCloser, error)
	// Delete permenantly removes the underlying resource
	Delete(cxt context.Context, url string, opts ...WriteOption) error
}
