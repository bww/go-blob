package blob

import (
	"context"
	"io"
)

type Service interface {
	// Init initializes a blob client in an implementation-specific way; for example, by creating the root path or GCS bucket it uses
	Init(cxt context.Context, opts ...WriteOption) error
	// Read obtains a stream to the specified resource
	Read(cxt context.Context, url string, opts ...ReadOption) (io.ReadCloser, error)
	// Accessor obtains a URL which provides access to the underlying resource; for example, a signed GCS URL
	Accessor(cxt context.Context, url string, opts ...ReadOption) (string, error)
	// Write obtains a writer which writes to the specified resource; if it does not exist, it is created; if it does exist it is overwritten
	Write(cxt context.Context, url string, opts ...WriteOption) (io.WriteCloser, error)
	// Delete permenantly removes the underlying resource
	Delete(cxt context.Context, url string, opts ...WriteOption) error
}
