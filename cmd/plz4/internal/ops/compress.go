package ops

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/prequel-dev/plz4"
	"github.com/prequel-dev/plz4/internal/pkg/descriptor"
)

const (
	// Give the tool some extra capacity for speed purposes;
	// effectively a tradeoff of CPU throughput for  RAM.
	pendingSize = -1
)

func RunCompress() error {

	rdwr, err := newTarget(true, CLI.Compress.File, CLI.Compress.Output, CLI.Compress.Force)

	if err != nil {
		return err
	}

	defer rdwr.Close()

	opts := []plz4.OptT{
		plz4.WithParallel(CLI.Cpus),
	}

	if CLI.Dict != "" {
		data, err := os.ReadFile(CLI.Dict)
		if err != nil {
			return fmt.Errorf("fail open dictionary file '%s':%w", CLI.Dict, err)
		}
		opts = append(opts, plz4.WithDictionary(data))
	}

	if CLI.Compress.CS {
		if CLI.Compress.File == "" {
			return errors.New("cannot get file size on stdin")
		}

		if rdwr.srcSz < 0 {
			return fmt.Errorf("cannot stat '%s': %w", CLI.Compress.File, err)
		}
		opts = append(opts, plz4.WithContentSize(uint64(rdwr.srcSz)))
	}

	bs, err := parseBlockSize(CLI.Compress.BS)
	if err != nil {
		return fmt.Errorf("invalid block size: %s", CLI.Compress.BS)
	}

	if CLI.Compress.Level < 0 || CLI.Compress.Level > 12 {
		return errors.New("compression level out of range")
	}

	opts = append(opts,
		plz4.WithLevel(plz4.LevelT(CLI.Compress.Level)),
		plz4.WithBlockChecksum(CLI.Compress.BX),
		plz4.WithContentChecksum(CLI.Compress.CX),
		plz4.WithBlockLinked(CLI.Compress.BD),
		plz4.WithBlockSize(bs),
	)

	return _compress(rdwr, opts...)
}

func _compress(rdwr *targetT, opts ...plz4.OptT) error {

	var (
		pw progress.Writer
		tr *progress.Tracker
	)

	if rdwr.Writer() != os.Stdout && !CLI.Compress.Quiet {
		msg := "Compressing"
		pw = newProgressWriter(1)
		pw.SetMessageLength(len(msg))

		tr = &progress.Tracker{
			Message: msg,
			Units:   progress.UnitsBytes,
		}

		if rdwr.srcSz > 0 {
			tr.Total = rdwr.srcSz
		}

		pw.AppendTracker(tr)

		cbHandler := func(srcOff, dstOff int64) {
			tr.SetValue(srcOff)
		}

		opts = append(opts,
			plz4.WithProgress(cbHandler),
			plz4.WithPendingSize(pendingSize),
		)

		go pw.Render()
	}

	var (
		start  = time.Now()
		wcnt   = &wrCnt{Writer: rdwr.Writer()}
		framer = plz4.NewWriter(wcnt, opts...)
	)

	n, err := io.Copy(framer, rdwr.Reader())
	if err != nil {
		return err
	}

	if err := framer.Close(); err != nil {
		return err
	}

	if pw != nil {
		tdiff := time.Since(start)

		tr.MarkAsDone()

		for pw.IsRenderInProgress() {
			time.Sleep(time.Millisecond * 100)
		}

		percent := float64(wcnt.cnt) / float64(n) * 100.0

		t := table.NewWriter()
		t.SetTitle("Compress results")
		t.SetStyle(table.StyleColoredBright)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Key", "Value"})

		input := strStdin
		if rdwr.src != nil {
			input = rdwr.src.Name()
		}

		t.AppendRows([]table.Row{
			{"Input", input},
			{"Output", rdwr.dst.Name()},
			{"InSize", n},
			{"OutSize", wcnt.cnt},
			{"Duration", tdiff.Round(time.Microsecond)},
			{"Ratio", fmt.Sprintf("%.2f%%", percent)},
		})
		t.Render()
	}
	return nil
}

type wrCnt struct {
	cnt uint64
	io.Writer
}

func (w *wrCnt) Write(data []byte) (n int, err error) {
	n, err = w.Writer.Write(data)
	if n >= 0 {
		w.cnt += uint64(n)
	}
	return
}

func parseBlockSize(bs string) (sz descriptor.BlockIdxT, err error) {
	switch bs {
	case "4MB", "4MiB", "4M":
		sz = descriptor.BlockIdx4MB
	case "1MB", "1MiB", "1M":
		sz = descriptor.BlockIdx1MB
	case "256KB", "246KiB", "256K":
		sz = descriptor.BlockIdx256KB
	case "64KB", "64KiB", "64K":
		sz = descriptor.BlockIdx64KB
	default:
		err = errors.New("fail parse block size")
	}
	return
}
