package main

import (
	"fmt"
	"io"
	"os"

	"github.com/cheggaaa/pb/v3"
	"github.com/mkideal/cli"
	"github.com/rubiojr/fsplit"
)

var help = cli.HelpCommand("Display help information")

type rootT struct {
	cli.Helper
	Quiet bool `cli:"quiet" usage:"be quiet"`
}

var rootCmd = &cli.Command{
	Desc: "fsplit is a file splitter and assembler",
	// Argv is a factory function of argument object
	// ctx.Argv() is if Command.Argv == nil or Command.Argv() is nil
	Argv: func() interface{} { return new(rootT) },
	Fn: func(ctx *cli.Context) error {
		//argv := ctx.Argv().(*rootT)
		return nil
	},
}

type assembleT struct {
	cli.Helper
	Manifest string `cli:"*manifest" usage:"manifest file"`
	Dst      string `cli:"*output" usage:"destination file"`
	Quiet    bool   `cli:"quiet" usage:"be quiet"`
}

var assembleCmd = &cli.Command{
	Name: "assemble",
	Desc: "Assemble chunks into a file",
	Argv: func() interface{} { return new(assembleT) },
	Fn: func(ctx *cli.Context) error {
		argv := ctx.Argv().(*assembleT)
		splitter := fsplit.DefaultSplitter()

		manifest, err := splitter.ReadManifest(argv.Manifest)
		exitIfErr(err)

		var h io.Writer
		h, err = os.Create(argv.Dst)
		defer h.(*os.File).Close()
		if !argv.Quiet {
			fmt.Println("Assembling", argv.Manifest, "into", argv.Dst, "...")
			fmt.Println("Size:", manifest.Size)
			bar := pb.Full.Start64(manifest.Size)
			exitIfErr(err)

			h = bar.NewProxyWriter(h)
			defer bar.Finish()
		}

		splitter.Assemble(manifest, h)
		return nil
	},
}

type splitT struct {
	cli.Helper
	Source         string `cli:"*file" usage:"source file to split"`
	ChunkDir       string `cli:"*chunk-dir" usage:"directory to store chunks"`
	CreateChunkDir bool   `cli:"create-chunk-dir" usage:"Create chunk directory if not exists"`
	Quiet          bool   `cli:"quiet" usage:"be quiet"`
	Parallel       bool   `cli:"parallel" usage:"split in parallel"`
}

var splitCmd = &cli.Command{
	Name: "split",
	Desc: "Split a file into chunks",
	Argv: func() interface{} { return new(splitT) },
	Fn: func(ctx *cli.Context) error {
		argv := ctx.Argv().(*splitT)

		if argv.CreateChunkDir {
			err := os.MkdirAll(argv.ChunkDir, os.ModePerm)
			exitIfErr(err)
		}

		splitter := fsplit.DefaultSplitter()

		var sf io.Reader
		sf, err := os.Open(argv.Source)
		defer sf.(*os.File).Close()
		exitIfErr(err)

		if !argv.Quiet {
			fmt.Println("Splitting", argv.Source, "into", argv.ChunkDir, "...")
			fi, err := sf.(*os.File).Stat()
			exitIfErr(err)

			bar := pb.Full.Start64(fi.Size())
			exitIfErr(err)
			sf = bar.NewProxyReader(sf)
			defer bar.Finish()
		}

		if argv.Parallel {
			_, err = splitter.SplitParallel(sf, argv.ChunkDir)
		} else {
			_, err = splitter.Split(sf, argv.ChunkDir)
		}
		exitIfErr(err)

		return nil
	},
}

func main() {
	if err := cli.Root(rootCmd,
		cli.Tree(help),
		cli.Tree(splitCmd),
		cli.Tree(assembleCmd),
	).Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func exitIfErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "err: "+err.Error())
		os.Exit(1)
	}
}
