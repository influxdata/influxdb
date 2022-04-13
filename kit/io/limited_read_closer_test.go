package io

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimitedReadCloser_Exceeded(t *testing.T) {
	b := &closer{Reader: bytes.NewBufferString("howdy")}
	rc := NewLimitedReadCloser(b, 3)

	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("how"), out)
	assert.Equal(t, ErrReadLimitExceeded, rc.Close())
}

func TestLimitedReadCloser_Happy(t *testing.T) {
	b := &closer{Reader: bytes.NewBufferString("ho")}
	rc := NewLimitedReadCloser(b, 2)

	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("ho"), out)
	assert.Nil(t, err)
}

func TestLimitedReadCloseWithErrorAndLimitExceeded(t *testing.T) {
	b := &closer{
		Reader: bytes.NewBufferString("howdy"),
		err:    errors.New("some error"),
	}
	rc := NewLimitedReadCloser(b, 3)

	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("how"), out)
	// LimitExceeded error trumps the close error.
	assert.Equal(t, ErrReadLimitExceeded, rc.Close())
}

func TestLimitedReadCloseWithError(t *testing.T) {
	closeErr := errors.New("some error")
	b := &closer{
		Reader: bytes.NewBufferString("howdy"),
		err:    closeErr,
	}
	rc := NewLimitedReadCloser(b, 10)

	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("howdy"), out)
	assert.Equal(t, closeErr, rc.Close())
}

func TestMultipleCloseOnlyClosesOnce(t *testing.T) {
	closeErr := errors.New("some error")
	b := &closer{
		Reader: bytes.NewBufferString("howdy"),
		err:    closeErr,
	}
	rc := NewLimitedReadCloser(b, 10)

	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, []byte("howdy"), out)
	assert.Equal(t, closeErr, rc.Close())
	assert.Equal(t, closeErr, rc.Close())
	assert.Equal(t, 1, b.closeCount)
}

type closer struct {
	io.Reader
	err        error
	closeCount int
}

func (c *closer) Close() error {
	c.closeCount++
	return c.err
}
