package cli

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var AddCmd = &cli.Command{
	Name:  "add",
	Usage: "add files",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		p, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return err
		}
		if !strings.HasPrefix(p, "/") {
			if dir, err := os.Getwd(); err == nil {
				p = filepath.Join(dir, p)
			}
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		pb, err := api.Add(ctx, p)
		if err != nil {
			return err
		}
		return PrintProgress(pb)

	},
}

var Add2Cmd = &cli.Command{
	Name:  "add2",
	Usage: "add files",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:    "batch-read",
			Aliases: []string{"br"},
			Usage:   "",
			Value:   32,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		p, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return err
		}
		if !strings.HasPrefix(p, "/") {
			if dir, err := os.Getwd(); err == nil {
				p = filepath.Join(dir, p)
			}
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		pb, err := api.Add2(ctx, p, cctx.Int("batch-read"))
		if err != nil {
			return err
		}
		return PrintProgress(pb)

	},
}
