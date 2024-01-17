package gcs

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
)

func TestGCSCRUD(t *testing.T) {
	var dsn string

	cxt, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	store, err := New(cxt, "gcs://treno-integration/bucket")
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	// we must create the bucket for testing; in normal usage, the bucket
	// is expected to exist before we interact with it
	_, err = store.bucket.Attrs(cxt)
	if err != nil {
		err = store.bucket.Create(cxt, store.projectId, &storage.BucketAttrs{})
		if !assert.NoError(t, err) {
			return
		}
	}

	d1 := `Hello, this is the data.`
	d2 := `Hello, this is the updated data.`

	// write a resource
	dsn = "file1"
	fmt.Printf("=> %s\n", dsn)
	w, err := store.Write(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}

	n, err := w.Write([]byte(d1))
	assert.NoError(t, err)
	assert.Equal(t, len(d1), n)
	err = w.Close()
	if !assert.NoError(t, err) {
		return
	}

	// this is the same resource, using the URL resource indicator
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("=> %s\n", dsn)
	w, err = store.Write(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}

	n, err = w.Write([]byte(d1))
	assert.NoError(t, err)
	assert.Equal(t, len(d1), n)
	err = w.Close()
	if !assert.NoError(t, err) {
		return
	}

	// the result must be the second file for both
	dsn = "file1"
	fmt.Printf("<= %s\n", dsn)
	r1, err := store.Read(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}
	defer r1.Close()

	d3, err := io.ReadAll(r1)
	assert.NoError(t, err)
	assert.Equal(t, d2, string(d3))

	// the result must be the second file for both
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("<= %s\n", dsn)
	r2, err := store.Read(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}
	defer r2.Close()

	d4, err := io.ReadAll(r2)
	assert.NoError(t, err)
	assert.Equal(t, d2, string(d4))

}
