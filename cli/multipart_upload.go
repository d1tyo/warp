package cli

import (
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
)

var multipartUploadFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "parts",
		Value: 100,
		Usage: "Number of parts to upload for each multipart upload",
	},
	cli.StringFlag{
		Name:  "part.size",
		Value: "5MiB",
		Usage: "Size of each part. Can be a number or MiB/GiB.",
	},
	cli.IntFlag{
		Name:  "part.concurrent",
		Value: 20,
		Usage: "Run this many concurrent operations per each multipart upload. Must not exceed a number of parts.",
	},
}

var MultiPartUploadCombinedFlags = combineFlags(globalFlags, ioFlags, multipartUploadFlags, genFlags, benchFlags, analyzeFlags)

// MultipartUpload command
var multipartUploadCmd = cli.Command{
	Name:   "multipart-put",
	Usage:  "benchmark multipart upload",
	Action: mainMutipartUpload,
	Before: setGlobalsFromContext,
	Flags:  MultiPartUploadCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#multipart-upload

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainMutipartUpload is the entry point for multipart-upload command
func mainMutipartUpload(ctx *cli.Context) error {
	checkMultipartUploadSyntax(ctx)

	b := &bench.MultipartUpload{
		Common:           getCommon(ctx, newGenSource(ctx, "part.size")),
		PartsNumber:      ctx.Int("parts"),
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

	if ctx.Int("parts") > 10000 {
		console.Fatal("parts can't be more than 10000")
	}
	if ctx.Int("parts") <= 0 {
		console.Fatal("parts must be at least 1")
	}

	if ctx.Int("part.concurrent") > ctx.Int("parts") {
		console.Fatal("part.concurrent can't be more than parts")
	}

	sz, err := toSize(ctx.String("part.size"))
	if err != nil {
		console.Fatal("error parsing part.size:", err)
	}
	if sz <= 0 {
		console.Fatal("part.size must be at least 1")
	}
}
