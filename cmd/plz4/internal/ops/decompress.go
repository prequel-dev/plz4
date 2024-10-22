package ops

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/prequel-dev/plz4"
	"github.com/prequel-dev/plz4/pkg/sparse"
)

func RunDecompress() error {
	rdwr, err := newTarget(false, CLI.Decompress.File, CLI.Decompress.Output, CLI.Decompress.Force)

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
			fmt.Fprintf(os.Stderr, "Fail open dictionary file '%s':%v\n", CLI.Dict, err)
			return err
		}
		opts = append(opts, plz4.WithDictionary(data))
	}

	return _decompress(rdwr, opts...)
}

func _decompress(rdwr *targetT, opts ...plz4.OptT) error {

	var (
		wr = rdwr.Writer()
		pw progress.Writer
		tr *progress.Tracker
	)

	if wr != os.Stdout && CLI.Decompress.Sparse {
		wr = sparse.NewWriter(wr)
	}

	if wr != os.Stdout && !CLI.Decompress.Quiet {
		msg := "Decompressing"
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

		opts = append(opts, plz4.WithProgress(cbHandler))

		go pw.Render()
	}

	var (
		start  = time.Now()
		rcnt   = &rdCnt{Reader: rdwr.Reader()}
		framer = plz4.NewReader(rcnt, opts...)
	)

	n, err := framer.WriteTo(wr)

	if err != nil {
		return err
	}

	if err := framer.Close(); err != nil {
		return err
	}

	// Framer does not close the underlying writer
	if err := wr.Close(); err != nil {
		return err
	}

	if pw != nil {
		tdiff := time.Since(start)

		tr.MarkAsDone()

		for pw.IsRenderInProgress() {
			time.Sleep(time.Millisecond * 100)
		}

		percent := float64(rcnt.cnt) / float64(n) * 100.0

		t := table.NewWriter()
		t.SetTitle("Decompress results")
		t.SetStyle(table.StyleColoredBright)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Key", "Value"})
		t.AppendRows([]table.Row{
			{"Input", rdwr.src.Name()},
			{"Output", rdwr.dst.Name()},
			{"InSize", rcnt.cnt},
			{"OutSize", n},
			{"Duration", tdiff.Round(time.Microsecond)},
			{"Ratio", fmt.Sprintf("%.2f%%", percent)},
		})
		t.Render()
	}
	return nil
}

type rdCnt struct {
	cnt uint64
	io.Reader
}

func (r *rdCnt) Read(data []byte) (n int, err error) {
	n, err = r.Reader.Read(data)
	if n >= 0 {
		r.cnt += uint64(n)
	}
	return
}
