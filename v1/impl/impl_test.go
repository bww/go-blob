package impl

import (
	"context"
	"testing"

	"github.com/bww/go-blob/v1"
	"github.com/stretchr/testify/assert"
)

func TestImpl(t *testing.T) {
	var err error
	cxt := context.Background()
	_, err = New(cxt, "gcs://test/bucket")
	assert.NoError(t, err)
	_, err = New(cxt, "file:///tmp/path")
	assert.NoError(t, err)
	_, err = New(cxt, "unsupported://doesnt-exist")
	assert.ErrorIs(t, err, blob.ErrNotSupported)
}
