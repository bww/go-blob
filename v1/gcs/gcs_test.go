package gcs

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGCSCRUD(t *testing.T) {
	store, err := New(context.Background(), "gcs://bucket")
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}
	if store == nil {
	}
}
