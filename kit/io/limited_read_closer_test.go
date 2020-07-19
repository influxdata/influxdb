package io

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimitedReadCloser_Exceeded(t *testing.T) {
	b := closer{bytes.NewBufferString("howdy")}
	rc := NewLimitedReadCloser(b, 2)

	out, err := ioutil.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("how"), out)
	assert.Equal(t, ErrReadLimitExceeded, rc.Close())
}

func TestLimitedReadCloser_Happy(t *testing.T) {
	b := closer{bytes.NewBufferString("ho")}
	rc := NewLimitedReadCloser(b, 2)

	out, err := ioutil.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("ho"), out)
	assert.NoError(t, err)
}

type closer struct {
	io.Reader
}

func (c closer) Close() error { return nil }
