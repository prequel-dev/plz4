package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/prequel-dev/plz4/cmd/plz4/internal/ops"
)

func main() {

	var (
		errS string
		kctx = kong.Parse(&ops.CLI)
	)

	switch kctx.Command() {
	case "compress", "compress <file>":
		if err := ops.RunCompress(); err != nil {
			errS = fmt.Sprintf("fail compress: %v", err)
		}
	case "decompress", "decompress <file>":
		if err := ops.RunDecompress(); err != nil {
			errS = fmt.Sprintf("fail decompress: %v", err)
		}
	case "verify", "verify <file>":
		if err := ops.RunVerify(); err != nil {
			errS = fmt.Sprintf("fail verify: %v", err)
		}
	case "bakeoff", "bakeoff <file>":
		if err := ops.RunBakeoff(); err != nil {
			errS = fmt.Sprintf("fail verify: %v", err)
		}
	default:
		errS = fmt.Sprintf("unknown command '%s'", kctx.Command())
	}

	if errS != "" {
		fmt.Fprintf(os.Stderr, "plz4: %s\n", errS)
		os.Exit(1)
	}
}
