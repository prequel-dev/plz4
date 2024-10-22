package ops

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pierrec/lz4/v4"
	"github.com/prequel-dev/plz4"
)

func RunBakeoff() error {

	rdwr, err := newTarget(true, CLI.Bakeoff.File, "-", false)

	if err != nil {
		return err
	}

	defer rdwr.Close()

	var (
		rdr = rdwr.Reader()
	)

	// Consume into RAM; must be able to seek
	if rdr == os.Stdin {
		var buf bytes.Buffer
		n, err := io.Copy(&buf, rdr)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(buf.Bytes())
		rdwr.srcSz = n
	}

	rds, ok := rdr.(io.ReadSeeker)
	if !ok {
		return errors.New("file not seekable")
	}

	if err := outputOptions(); err != nil {
		return err
	}

	fmt.Println()

	pw := newProgressWriter(2)
	go pw.Render()

	plz4Baker, err := _prepPlz4(rds, rdwr.srcSz, pw)
	if err != nil {
		return err
	}

	lz4Baker, err := _prepLz4(rds, rdwr.srcSz, pw)
	if err != nil {
		fmt.Printf("Fail to bake lz4: %v\n", err)
	}

	var (
		plz4Results []resultT
		lz4Results  []resultT
	)

	if plz4Baker != nil {
		if plz4Results, err = plz4Baker(); err != nil {
			return err
		}
	}

	if lz4Baker != nil {
		if lz4Results, err = lz4Baker(); err != nil {
			return err
		}
	}

	for pw.IsRenderInProgress() {
		time.Sleep(time.Millisecond * 100)
	}

	return outputResults(rdwr.srcSz, plz4Results, lz4Results)
}

func newProgressWriter(nTrackers int) progress.Writer {
	pw := progress.NewWriter()
	pw.SetAutoStop(true)
	pw.SetMessageLength(24)
	pw.SetNumTrackersExpected(nTrackers)
	pw.SetSortBy(progress.SortByPercentDsc)
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerLength(25)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(time.Millisecond * 100)
	pw.Style().Colors = progress.StyleColorsExample
	pw.Style().Options.PercentFormat = "%4.1f%%"
	pw.Style().Visibility.ETA = true
	pw.Style().Visibility.Percentage = true
	pw.Style().Visibility.Speed = true
	pw.Style().Visibility.Time = true
	return pw
}

func outputOptions() error {
	t := table.NewWriter()
	t.SetTitle("Bakeoff Configuration")
	t.SetStyle(table.StyleColoredBright)
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Option", "Value"})

	fn := strStdin
	if CLI.Bakeoff.File != "" {
		fn = CLI.Bakeoff.File
	}

	dict := strUnset
	if CLI.Dict != "" {
		dict = CLI.Dict
	}

	t.AppendRows([]table.Row{
		{"File name", fn},
		{"Dictionary", dict},
		{"Concurrency", CLI.Cpus},
		{"Block Size", CLI.Bakeoff.BS},
		{"Block Checksum", CLI.Bakeoff.BX},
		{"Blocks Linked", CLI.Bakeoff.BD},
		{"Content Checksum", CLI.Bakeoff.CS},
		{"Content Size", CLI.Bakeoff.CX},
	})

	t.Render()
	return nil
}

func outputResults(srcSz int64, plz4Results, lz4Results []resultT) error {
	fmt.Println()

	t := table.NewWriter()
	t.SetTitle("Bakeoff Results")
	t.SetStyle(table.StyleColoredBright)
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Algo", "Level", "SrcSize", "Compressed", "Ratio", "Compress", "Decompress"})
	for i, r := range plz4Results {
		percent := fmt.Sprintf("%.1f%%", float64(r.cnt)/float64(srcSz)*100.0)
		t.AppendRow([]interface{}{"plz4", i + 1, srcSz, r.cnt, percent, r.dur.Round(time.Microsecond), r.ddur.Round(time.Microsecond)})
	}

	t.AppendSeparator()

	for i, r := range lz4Results {
		percent := fmt.Sprintf("%.1f%%", float64(r.cnt)/float64(srcSz)*100.0)
		t.AppendRow([]interface{}{"lz4", i, srcSz, r.cnt, percent, r.dur.Round(time.Microsecond), r.ddur.Round(time.Microsecond)})
	}

	t.Render()
	return nil
}

type resultT struct {
	cnt  int64
	dur  time.Duration
	ddur time.Duration
}

func _prepLz4(rd io.ReadSeeker, srcSz int64, pw progress.Writer) (bakeFuncT, error) {

	opts, err := _parseBakeLz4Opts(srcSz)
	if err != nil {
		return nil, err
	}

	var (
		i = 0
	)

	bsz, err := parseBlockSize(CLI.Bakeoff.BS)
	if err != nil {
		return nil, err
	}
	bss := int64(bsz.Size())

	tr := &progress.Tracker{
		Message: "Processing lz4",
		Total:   srcSz * 10,
		Units:   progress.UnitsBytes,
	}

	pw.AppendTracker(tr)

	// lz4 callback on write is odd, returns compressed block size
	// Work around by assuming incremental block size and fix up for overflow
	cbHandler := func(_ int) {
		tr.Increment(bss)
	}

	opts = append(opts, lz4.OnBlockDoneOption(cbHandler))

	bakeFunc := func() ([]resultT, error) {
		defer tr.MarkAsDone()

		var results []resultT

		for ; i < 10; i++ {
			start := time.Now()

			if _, err := rd.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}

			// Last one wins; so append is ok.
			lvl, err := lz4Level(i)
			if err != nil {
				return nil, err
			}

			opts = append(opts, lz4.CompressionLevelOption(lvl))

			split, cnt, err := lz4BakeOne(rd, opts...)
			if err != nil {
				return nil, err
			}

			var (
				ddur = time.Since(split)
				cdur = split.Sub(start)
			)

			results = append(results, resultT{
				dur:  cdur,
				cnt:  cnt,
				ddur: ddur,
			})
		}

		return results, nil
	}

	return bakeFunc, nil
}

type bakeFuncT func() ([]resultT, error)

func lz4BakeOne(src io.Reader, opts ...lz4.Option) (split time.Time, cnt int64, err error) {
	var (
		fh *os.File
		wr io.Writer
		rd io.Reader
	)

	if CLI.Bakeoff.RAM {
		buf := &bytes.Buffer{}
		wr = buf
		rd = buf
	} else {
		fh, err = os.CreateTemp("", "lz4_bake")
		if err != nil {
			return
		}
		defer os.Remove(fh.Name())
		wr = fh
		rd = fh
	}

	var (
		wcnt   = &wrCnt{Writer: wr}
		framer = lz4.NewWriter(wcnt)
	)
	framer.Apply(opts...)

	_, err = io.Copy(framer, src)
	if err != nil {
		return
	}

	if err = framer.Close(); err != nil {
		return
	}

	split = time.Now()

	if fh != nil {
		if _, err = fh.Seek(0, io.SeekStart); err != nil {
			return
		}
	}

	// Now decompress
	err = _lz4Decompress(rd)
	cnt = int64(wcnt.cnt)
	return
}

func _prepPlz4(rd io.ReadSeeker, srcSz int64, pw progress.Writer) (bakeFuncT, error) {

	opts, err := _parseBakePlz4Opts(srcSz)
	if err != nil {
		return nil, err
	}

	tr := &progress.Tracker{
		Message: "Processing plz4",
		Total:   srcSz * 12,
		Units:   progress.UnitsBytes,
	}

	pw.AppendTracker(tr)

	bakeFunc := func() ([]resultT, error) {
		defer tr.MarkAsDone()

		var i = 0

		cbHandler := func(srcOff, dstOff int64) {
			tr.SetValue(srcOff + (int64(i) * srcSz))
		}

		opts = append(opts,
			plz4.WithProgress(cbHandler),
			plz4.WithPendingSize(-1),
		)

		var results []resultT

		for ; i < 12; i++ {
			start := time.Now()

			if _, err := rd.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}

			// Last one wins; so append is ok.
			opts = append(opts,
				plz4.WithLevel(plz4.LevelT(i+1)),
			)

			split, cnt, err := plz4BakeOne(rd, opts...)
			if err != nil {
				return nil, err
			}

			var (
				ddur = time.Since(split)
				cdur = split.Sub(start)
			)

			results = append(results, resultT{
				dur:  cdur,
				cnt:  cnt,
				ddur: ddur,
			})
		}

		return results, nil
	}

	return bakeFunc, nil
}

func plz4BakeOne(src io.Reader, opts ...plz4.OptT) (split time.Time, cnt int64, err error) {
	var (
		fh *os.File
		wr io.Writer
		rd io.Reader
	)

	if CLI.Bakeoff.RAM {
		buf := &bytes.Buffer{}
		wr = buf
		rd = buf
	} else {
		fh, err = os.CreateTemp("", "plz4_bake")
		if err != nil {
			return
		}
		defer os.Remove(fh.Name())
		wr = fh
		rd = fh
	}

	var (
		wcnt   = &wrCnt{Writer: wr}
		framer = plz4.NewWriter(wcnt, opts...)
	)

	_, err = io.Copy(framer, src)
	if err != nil {
		return
	}

	if err = framer.Close(); err != nil {
		return
	}

	split = time.Now()

	if fh != nil {
		if _, err = fh.Seek(0, io.SeekStart); err != nil {
			return
		}
	}

	// Now decompress
	err = _plz4Decompress(rd)
	cnt = int64(wcnt.cnt)
	return
}

func _plz4Decompress(rd io.Reader) error {

	opts := []plz4.OptT{
		plz4.WithParallel(CLI.Cpus),
		plz4.WithPendingSize(-1),
	}

	if CLI.Dict != "" {
		data, err := os.ReadFile(CLI.Dict)
		if err != nil {
			return fmt.Errorf("fail open dictionary file '%s':%w", CLI.Dict, err)
		}
		opts = append(opts, plz4.WithDictionary(data))
	}

	frd := plz4.NewReader(rd, opts...)
	_, err := frd.WriteTo(io.Discard)
	if err == nil {
		err = frd.Close()
	}

	return err
}

func _lz4Decompress(rd io.Reader) error {

	frd := lz4.NewReader(rd)

	if CLI.Cpus != 0 {
		frd.Apply(lz4.ConcurrencyOption(CLI.Cpus))
	}

	_, err := frd.WriteTo(io.Discard)

	return err
}

func _parseBakePlz4Opts(srcSz int64) ([]plz4.OptT, error) {

	opts := []plz4.OptT{
		plz4.WithParallel(CLI.Cpus),
	}

	if CLI.Dict != "" {
		data, err := os.ReadFile(CLI.Dict)
		if err != nil {
			return nil, fmt.Errorf("fail open dictionary file '%s':%w", CLI.Dict, err)
		}
		opts = append(opts, plz4.WithDictionary(data))
	}

	if CLI.Bakeoff.CS {
		if CLI.Bakeoff.File == "" {
			return nil, errors.New("cannot get file size on stdin")
		}

		if srcSz < 0 {
			return nil, fmt.Errorf("cannot stat '%s'", CLI.Bakeoff.File)
		}
		opts = append(opts, plz4.WithContentSize(uint64(srcSz)))
	}

	bs, err := parseBlockSize(CLI.Bakeoff.BS)
	if err != nil {
		return nil, fmt.Errorf("invalid block size: %s", CLI.Bakeoff.BS)
	}

	return append(opts,
		plz4.WithBlockChecksum(CLI.Bakeoff.BX),
		plz4.WithContentChecksum(CLI.Bakeoff.CX),
		plz4.WithBlockLinked(CLI.Bakeoff.BD),
		plz4.WithBlockSize(bs),
	), nil
}

func _parseBakeLz4Opts(srcSz int64) ([]lz4.Option, error) {

	var opts []lz4.Option

	if CLI.Cpus != 0 {
		opts = append(opts, lz4.ConcurrencyOption(CLI.Cpus))
	}

	if CLI.Dict != "" {
		return nil, errors.New("dictionary compress not supported")
	}

	if CLI.Bakeoff.BD {
		return nil, errors.New("linked blocks not supported")
	}

	if CLI.Bakeoff.CS {
		if srcSz < 0 {
			return nil, fmt.Errorf("cannot stat '%s'", CLI.Bakeoff.File)
		}
		opts = append(opts, lz4.SizeOption(uint64(srcSz)))
	}

	bs, err := parseBlockSize(CLI.Bakeoff.BS)
	if err != nil {
		return nil, fmt.Errorf("invalid block size: %s", CLI.Bakeoff.BS)
	}

	var lbz lz4.BlockSize
	switch bs {
	case plz4.BlockIdx1MB:
		lbz = lz4.Block1Mb
	case plz4.BlockIdx256KB:
		lbz = lz4.Block256Kb
	case plz4.BlockIdx4MB:
		lbz = lz4.Block4Mb
	case plz4.BlockIdx64KB:
		lbz = lz4.Block64Kb
	default:
		return nil, errors.New("fail map block size")
	}

	return append(opts,
		lz4.BlockChecksumOption(CLI.Bakeoff.BX),
		lz4.ChecksumOption(CLI.Bakeoff.CX),
		lz4.BlockSizeOption(lbz),
	), nil
}

func lz4Level(l int) (lz4.CompressionLevel, error) {

	// Last one wins; so append is ok.
	var lz4Level lz4.CompressionLevel
	switch l {
	case 0:
		lz4Level = lz4.Fast
	case 1:
		lz4Level = lz4.Level1
	case 2:
		lz4Level = lz4.Level2
	case 3:
		lz4Level = lz4.Level3
	case 4:
		lz4Level = lz4.Level4
	case 5:
		lz4Level = lz4.Level5
	case 6:
		lz4Level = lz4.Level6
	case 7:
		lz4Level = lz4.Level7
	case 8:
		lz4Level = lz4.Level8
	case 9:
		lz4Level = lz4.Level9
	default:
		return 0, errors.New("fail map lz4 compression level")
	}
	return lz4Level, nil
}
