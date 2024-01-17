package blob

import "errors"

var (
	ErrNotFound     = errors.New("Not found")
	ErrInvalidURL   = errors.New("Invalid URL")
	ErrNotSupported = errors.New("Not supported")
)
