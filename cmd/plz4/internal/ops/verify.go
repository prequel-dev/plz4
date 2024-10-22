package ops

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/prequel-dev/plz4"
	"github.com/prequel-dev/plz4/internal/pkg/header"
)

const (
	strStdin = "<STDIN>"
	strUnset = "<UNSET>"
)

func RunVerify() error {
	rdwr, err := newTarget(false, CLI.Verify.File, "-", false)

	if err != nil {
		return err
	}

	defer rdwr.Close()

	opts := []plz4.OptT{
		plz4.WithParallel(CLI.Cpus),
		plz4.WithPendingSize(pendingSize),
	}

	if CLI.Dict != "" {
		data, err := os.ReadFile(CLI.Dict)
		if err != nil {
			return fmt.Errorf("fail open dictionary file '%s':%w", CLI.Dict, err)
		}
		opts = append(opts, plz4.WithDictionary(data))
	}

	return _verify(rdwr, opts...)
}

func _verify(rdwr *targetT, opts ...plz4.OptT) error {
	if CLI.Verify.Skip {
		return _skipVerify(rdwr)
	}

	var (
		hdrBuf headerBuf
		tee    = io.TeeReader(rdwr.Reader(), &hdrBuf)
		rcnt   = &rdCnt{Reader: tee}
	)

	msg := "Verifying"
	pw := newProgressWriter(1)
	pw.SetMessageLength(len(msg))

	tr := &progress.Tracker{
		Message: msg,
		Units:   progress.UnitsBytes,
	}

	if rdwr.srcSz > 0 {
		tr.Total = rdwr.srcSz
	}

	pw.AppendTracker(tr)
	go pw.Render()

	cbHandler := func(srcOff, dstOff int64) {
		tr.SetValue(srcOff)
	}

	skipCB := func(rdr io.Reader, nibble uint8, sz uint32) (int, error) {
		fmt.Fprintf(rdwr.Writer(), "Skip detected offset:%v nibble:%v sz:%d\n", rcnt.cnt, nibble, sz)
		n, err := io.CopyN(io.Discard, rdr, int64(sz))
		return int(n), err
	}

	opts = append(
		opts,
		plz4.WithSkipCallback(skipCB),
		plz4.WithProgress(cbHandler),
	)

	var (
		start  = time.Now()
		framer = plz4.NewReader(rcnt, opts...)
		n, err = framer.WriteTo(io.Discard)
		wr     = rdwr.Writer()
	)

	tdiff := time.Since(start)

	tr.MarkAsDone()

	for pw.IsRenderInProgress() {
		time.Sleep(time.Millisecond * 100)
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleColoredBright)
	t.SetOutputMirror(os.Stdout)
	t.SetTitle("Verify results")
	t.AppendHeader(table.Row{"Key", "Value"})

	var (
		fileName   = strStdin
		dictionary = strUnset
		percent    = float64(rcnt.cnt) / float64(n) * 100.0
	)

	if rdwr.src != nil {
		fileName = rdwr.src.Name()
	}

	if CLI.Dict != "" {
		dictionary = CLI.Dict
	}

	t.AppendRows([]table.Row{
		{"File name", fileName},
		{"Dictionary", dictionary},
		{"InSize", rcnt.cnt},
		{"OutSize", n},
		{"Duration", tdiff.Round(time.Microsecond)},
		{"Ratio", fmt.Sprintf("%.2f%%", percent)},
	})

	// Attempt to reparse the cached header
	if err == nil && hdrBuf.buf.Len() > 0 {
		t.AppendSeparator()
		err = verifyHeader(&hdrBuf.buf, t)
	}

	if err == nil {
		err = framer.Close()
	}

	if err != nil {
		return err
	} else if rcnt.cnt == 0 {
		fmt.Fprintf(wr, "No data to verify\n")
		return nil
	}

	t.Render()

	return nil
}

func _skipVerify(rdwr *targetT) error {
	t := table.NewWriter()
	t.SetStyle(table.StyleColoredBright)
	t.SetOutputMirror(os.Stdout)
	t.SetTitle("Header Metadata")
	t.AppendHeader(table.Row{"Key", "Value"})

	if err := verifyHeader(rdwr.Reader(), t); err != nil {
		return err
	}

	t.Render()
	return nil
}

func verifyHeader(rd io.Reader, tw table.Writer) error {
	_, hdr, err := header.ReadHeader(rd, nil)
	if err != nil {
		return err
	}

	var (
		dictId    = strUnset
		contentSz = strUnset
	)
	if hdr.Flags.DictId() {
		dictId = fmt.Sprintf("%d", hdr.DictId)
	}
	if hdr.Flags.ContentSize() {
		contentSz = fmt.Sprintf("%d", hdr.ContentSz)
	}

	tw.AppendRows([]table.Row{
		{"Frame version", hdr.Flags.Version()},
		{"Dict Identifier", dictId},
		{"Content size", contentSz},
		{"Content checksum", hdr.Flags.ContentChecksum()},
		{"Block Size", hdr.BlockDesc.Idx().Str()},
		{"Block Checksum", hdr.Flags.BlockChecksum()},
		{"Block Independence", hdr.Flags.BlockIndependence()},
	})

	return nil
}

type headerBuf struct {
	buf bytes.Buffer
}

func (b *headerBuf) Write(data []byte) (int, error) {
	// Cache enough to grab header
	if b.buf.Len() < 19 {
		b.buf.Write(data)
	}
	return len(data), nil
}
