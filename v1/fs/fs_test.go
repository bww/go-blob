package fs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/bww/go-util/v1/errors"
	"github.com/bww/go-util/v1/text"
	"github.com/stretchr/testify/assert"
)

func TestFSCRUD(t *testing.T) {
	var dsn string

	cxt, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	root := text.Coalesce(os.Getenv("GOBLOB_FS_ROOT"), errors.Must(os.Getwd()))
	base := "file://" + root

	store, err := NewWithConfig(cxt, base, Config{Logger: slog.Default()})
	if !assert.Nil(t, err, fmt.Sprint(err)) {
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
	dsn = base + "/file1"
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
	dsn = base + "/file1"
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

	// obtain an accessor for the resource, which is just a file:// url
	dsn = "file1"
	fmt.Printf("<= %s\n", dsn)
	a1, err := store.Accessor(cxt, dsn)
	assert.NoError(t, err)
	assert.Equal(t, base+"/file1", a1)

	// do it the other way
	dsn = base + "/file1"
	fmt.Printf("<= %s\n", dsn)
	a2, err := store.Accessor(cxt, dsn)
	assert.NoError(t, err)
	assert.Equal(t, base+"/file1", a2)

	// delete our file
	dsn = "file1"
	fmt.Printf("~~ %s\n", dsn)
	err = store.Delete(cxt, dsn)
	if !assert.NoError(t, err) {
		return
	}

	// it shouldn't exist now
	dsn = base + "/file1"
	fmt.Printf("~~ %s\n", dsn)
	err = store.Delete(cxt, dsn)
	assert.NotNil(t, err)

	// it still shouldn't exist now
	dsn = base + "/file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Read(cxt, dsn)
	assert.NotNil(t, err)

	// accessors also don't work on nonexistent files
	dsn = base + "/file1"
	fmt.Printf("<= %s\n", dsn)
	_, err = store.Accessor(cxt, dsn)
	assert.NotNil(t, err)

}
