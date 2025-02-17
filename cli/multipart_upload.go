package cli

import (
	"fmt"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

var multipartUploadFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "obj.size",
		Value: "5MiB",
		Usage: "Size of each multipart object. Can be a number or MiB/GiB. Must be a single value",
	},
	cli.StringFlag{
		Name:  "part.size",
		Value: "5MiB",
		Usage: "Size of each part. Can be a number or MiB/GiB. Must be a single value >= 5MiB",
	},
	cli.IntFlag{
		Name:  "part.concurrent",
		Value: 20,
		Usage: "Run this many concurrent operations per each multipart upload. Must not exceed obj.size/part.size",
	},
}

var MultiPartUploadCombinedFlags = combineFlags(globalFlags, ioFlags, multipartUploadFlags, genFlags, benchFlags, analyzeFlags)

// MultipartUpload command
var multipartUploadCmd = cli.Command{
	Name:   "multipart-upload",
	Usage:  "benchmark multipart upload",
	Action: mainMutipartUpload,
	Before: setGlobalsFromContext,
	Flags:  MultiPartUploadCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// TODO(dtyo): add description of multipart-upload mode to README.md and add a link to the usage description above

// mainMutipartUpload is the entry point for multipart-upload command
func mainMutipartUpload(ctx *cli.Context) error {
	checkMultipartUploadSyntax(ctx)

	objSize, err := toSize(ctx.String("obj.size"))
	if err != nil {
		return fmt.Errorf("converting obj.size to size: %w", err)
	}

	b := &bench.MultipartUpload{
		Common:           getCommon(ctx, newGenSource(ctx, "part.size")),
		ObjectSize:       objSize,
		PartsConcurrency: ctx.Int("part.concurrent"),
	}
	return runBench(ctx, b)
}

func checkMultipartUploadSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Bool("disable-multipart") {
		console.Fatal("cannot disable multipart for multipart-upload test")
	}

	objSize := parseSingleSize(ctx, "obj.size")
	partSize := parseSingleSize(ctx, "part.size")

	if partSize < 5<<20 {
		console.Fatal("part.size must be >= 5MiB")
	}
	if objSize <= partSize {
		console.Fatal("part.size must be less than obj.size")
	}
	if objSize%partSize != 0 {
		console.Fatal("obj.size must be divisible by part.size", objSize, partSize)
	}
	if ctx.Uint64("part.concurrent") > objSize/partSize {
		console.Fatalf("part.concurrent is too much for a given obj.size and part.size. Must be not greater than %v\n", objSize/partSize)
	}
}

func parseSingleSize(ctx *cli.Context, sizeField string) uint64 {
	if strings.IndexRune(ctx.String(sizeField), ':') >= 0 || strings.IndexRune(ctx.String(sizeField), ',') >= 0 {
		console.Fatalf("%q must be a single size value\n")
	}
	sz, err := toSize(ctx.String(sizeField))
	if err != nil {
		console.Fatalf("error parsing %q: %v\n", sizeField, err)
	}

	return sz
}
