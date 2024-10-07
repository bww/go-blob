package gcs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	siter "github.com/bww/go-iterator/v1"
	"github.com/stretchr/testify/assert"
)

func TestGCSCRUD(t *testing.T) {
	var dsn string

	cxt, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	store, err := NewWithConfig(cxt, "gcs://treno-integration/bucket", Config{Logger: slog.Default()})
	if !assert.NoError(t, err) {
		return
	}

	// init creates the bucket we're using if it doesn't already exist
	err = store.Init(cxt)
	if !assert.NoError(t, err) {
		return
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

	n, err = w.Write([]byte(d2)) // write second version
	assert.NoError(t, err)
	assert.Equal(t, len(d2), n)
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

	d3, err := io.ReadAll(r1)
	assert.NoError(t, err)
	assert.Equal(t, d2, string(d3))

	err = r1.Close()
	if !assert.NoError(t, err) {
		return
	}

	// the result must be the second file for both
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("<= %s\n", dsn)
	r2, err := store.Read(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}

	d4, err := io.ReadAll(r2)
	assert.NoError(t, err)
	assert.Equal(t, d2, string(d4))

	err = r2.Close()
	if !assert.NoError(t, err) {
		return
	}

	// create some files under a directory
	dsn = "A/file1"
	fmt.Printf("<= %s\n", dsn)
	w, err = store.Write(cxt, dsn)
	assert.NoError(t, err)

	n, err = w.Write([]byte(d1))
	assert.NoError(t, err)
	assert.Equal(t, len(d1), n)
	assert.NoError(t, w.Close())

	dsn = "A/B/file1"
	fmt.Printf("<= %s\n", dsn)
	w, err = store.Write(cxt, dsn)
	assert.NoError(t, err)

	n, err = w.Write([]byte(d1))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	tree := make(map[string]struct{})
	iter, err := store.List(cxt, "A/")
	if assert.NoError(t, err) {
		for {
			rc, err := iter.Next()
			if siter.IsFinished(err) {
				break
			} else if !assert.NoError(t, err) {
				break
			}
			p := rc.URL
			fmt.Printf("<... %v\n", p)
			tree[p] = struct{}{}
		}
	}

	assert.Equal(t, map[string]struct{}{
		"A/file1":   {},
		"A/B/file1": {},
	}, tree)

	// this doesn't work under emulation; we expect failure but we should try to improve this
	dsn = "file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Accessor(cxt, dsn)
	assert.NotNil(t, err)

	// same here
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Accessor(cxt, dsn)
	assert.NotNil(t, err)

	// delete our file
	dsn = "file1"
	fmt.Printf("~~ %s\n", dsn)
	err = store.Delete(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}

	// it shouldn't exist now
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("~~ %s\n", dsn)
	err = store.Delete(cxt, dsn)
	assert.NotNil(t, err)

	// it still shouldn't exist now
	dsn = "file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Read(cxt, dsn)
	assert.NotNil(t, err)

	// check it this way
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Read(cxt, dsn)
	assert.NotNil(t, err)

	// this one either
	dsn = "gcs://treno-integration/bucket/file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Accessor(cxt, dsn)
	assert.NotNil(t, err)

}
