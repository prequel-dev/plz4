package ops

var CLI struct {
	Compress struct {
		File   string `optional:"" arg:"" type:"existingfile"`
		Output string `help:"Output filename; use '-' for stdout" short:"o"`
		Level  int    `help:"Compression level (1-12) [1 Fastest]" default:"1" short:"l"`
		Force  bool   `help:"Force overwrite of existing file" short:"f"`
		Quiet  bool   `help:"Do not write progress to stdout" short:"q"`
		BS     string `help:"Block size [4MB, 1MB, 256KB, 64KB]" default:"4MB"`
		BD     bool   `help:"Enable linked blocks"`
		BX     bool   `help:"Enable block checksum"`
		CX     bool   `help:"Enable content checksum"`
		CS     bool   `help:"Enable content size; fails on stdin"`
	} `cmd:"" aliases:"c,comp" help:"Compress data into lz4"`
	Decompress struct {
		File   string `optional:"" arg:"" type:"existingfile"`
		Output string `help:"Output filename; use '-' for stdout" short:"o"`
		Force  bool   `help:"Force overwrite of existing file" short:"f"`
		Quiet  bool   `help:"Do not write progress to stdout" short:"q"`
		Sparse bool   `help:"Enable sparse writes" short:"s"`
	} `cmd:"" aliases:"d,decomp" help:"Decompress lz4 data"`
	Verify struct {
		File string `optional:"" arg:"" type:"existingfile"`
		Skip bool   `help:"Skip decompress" short:"s"`
	} `cmd:"" aliases:"v,ver" help:"Verify lz4 data"`
	Bakeoff struct {
		File      string `optional:"" arg:"" type:"existingfile"`
		BS        string `help:"Block size [4MB, 1MB, 256KB, 64KB]" default:"4MB"`
		BD        bool   `help:"Enable linked blocks"`
		BX        bool   `help:"Enable block checksum"`
		CX        bool   `help:"Enable content checksum"`
		CS        bool   `help:"Enable content size; fails on stdin"`
		RAM       bool   `help:"Process data in RAM"`
		BlockMode bool   `help:"Use block API instead of frame API" short:"B"`
	} `cmd:"" aliases:"b,bake" help:"Compare performance to github.com/pierrec/lz4"`

	Cpus int    `help:"Concurrency [0 synchronous] [-1 auto]" default:"-1" short:"c"`
	Dict string `help:"Optional dictionary file" type:"existingfile"`
}
