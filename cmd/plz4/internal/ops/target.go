package ops

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	lz4Ext   = ".lz4"
	dstPerms = 0600
	dstFlags = os.O_CREATE | os.O_RDWR | os.O_TRUNC
)

type targetT struct {
	src   *os.File
	srcSz int64
	dst   *os.File
}

func newTarget(compress bool, name, output string, forceOverwrite bool) (*targetT, error) {

	var (
		err   error
		srcFh *os.File
		dstFh *os.File
	)

	defer func() {
		if srcFh != nil {
			srcFh.Close()
		}
		if dstFh != nil {
			dstFh.Close()
		}
	}()

	srcSz := int64(-1)
	if name != "" && name != "-" {
		if srcFh, err = os.Open(name); err != nil {
			return nil, fmt.Errorf("cannot open source '%s': %w", name, err)
		}

		// Try to grab size of the source for the progress bar
		if fi, err := srcFh.Stat(); err == nil {
			srcSz = fi.Size()
		}
	}

	if output != "-" {
		var (
			dstName string
		)
		switch {
		case output != "":
			dstName = output
		case compress:
			if name == "" || name == "-" {
				name = "out"
			}
			dstName = name + lz4Ext
		case strings.HasSuffix(name, lz4Ext):
			dstName = strings.TrimSuffix(name, lz4Ext)
		default:
			return nil, fmt.Errorf("cannot determine an output filename for '%s'", name)
		}

		if fileExists(dstName) && !forceOverwrite {
			return nil, fmt.Errorf("output file '%s' already exists", dstName)
		}

		if dstFh, err = os.OpenFile(dstName, dstFlags, dstPerms); err != nil {
			return nil, fmt.Errorf("fail create output file '%s': %w", dstName, err)
		}
	}

	tt := &targetT{
		src:   srcFh,
		srcSz: srcSz,
		dst:   dstFh,
	}

	srcFh = nil
	dstFh = nil
	return tt, nil
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return (err == nil) || !errors.Is(err, os.ErrNotExist)
}

func (t *targetT) Close() error {
	var errList []error
	if t.src != nil {
		if err := t.src.Close(); err != nil {
			errList = append(errList, err)
		}
		t.src = nil
	}
	if t.dst != nil {
		if err := t.dst.Close(); err != nil {
			errList = append(errList, err)
		}
		t.src = nil
	}
	return errors.Join(errList...)
}

func (t *targetT) Reader() io.Reader {
	if t.src == nil {
		return os.Stdin
	}
	return t.src
}

func (t *targetT) Writer() io.WriteCloser {
	if t.dst == nil {
		return os.Stdout
	}
	return t.dst
}
