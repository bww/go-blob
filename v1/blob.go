package blob

import (
	"context"
	"io"
)

type Service interface {
	Read(cxt context.Context, url string, opts ...ReadOption) (io.ReadCloser, error)
	Accessor(cxt context.Context, url string, opts ...ReadOption) (string, error)
	Write(cxt context.Context, url string, opts ...WriteOption) (io.WriteCloser, error)
	Delete(cxt context.Context, url string, opts ...WriteOption) error
}
