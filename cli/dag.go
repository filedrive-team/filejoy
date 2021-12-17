package cli

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var DagCmd = &cli.Command{
	Name:  "dag",
	Usage: "Manage dag",
	Subcommands: []*cli.Command{
		DagStat,
		DagSync,
		DagExport,
		DagHas,
	},
}

var DagHas = &cli.Command{
	Name:  "has",
	Usage: "check if local block store has dag",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		cid, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return err
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		has, err := api.DagHas(ctx, cid)
		if err != nil {
			return err
		}
		if has {
			fmt.Printf("has %s\n", cid)
		} else {
			fmt.Printf("hasn't %s\n", cid)
		}

		return nil
	},
}

var DagStat = &cli.Command{
	Name:  "stat",
	Usage: "print dag info",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		cid, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return err
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		stat, err := api.DagStat(ctx, cid)
		if err != nil {
			return err
		}
		fmt.Printf("%v\n", stat)

		return nil
	},
}

var DagSync = &cli.Command{
	Name:  "sync",
	Usage: "sync dags",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:    "concurrent",
			Aliases: []string{"c"},
			Usage:   "",
			Value:   32,
		},
		&cli.StringFlag{
			Name:  "f",
			Usage: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		cidsFilePath := cctx.String("f")

		if cidsFilePath != "" {
			if bs, err := ioutil.ReadFile(cidsFilePath); err == nil {
				cidlist := strings.Split(string(bs), "\n")
				for _, cidstr := range cidlist {
					cidstr = strings.TrimSpace(cidstr)
					if cidstr != "" {
						args = append(args, cidstr)
					}
				}
			}
		}

		var cids = make([]cid.Cid, 0)
		for _, cidstr := range args {
			cid, err := cid.Decode(cidstr)
			if err != nil {
				return err
			}
			cids = append(cids, cid)
		}
		fmt.Println(cids)

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		msgch, err := api.DagSync(ctx, cids, cctx.Int("concurrent"))
		if err != nil {
			return err
		}
		for msg := range msgch {
			fmt.Println(msg)
		}

		return nil
	},
}

var DagExport = &cli.Command{
	Name:  "export",
	Usage: "export car or padded car file",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:    "pad",
			Aliases: []string{"p"},
			Usage:   "filecoin piece pad",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		if len(args) < 2 {
			log.Info("usage: filejoy get [cid] [path]")
			return nil
		}
		cid, err := cid.Decode(args[0])
		if err != nil {
			return err
		}
		p, err := homedir.Expand(args[1])
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

		pb, err := api.DagExport(ctx, cid, p, cctx.Bool("pad"))
		if err != nil {
			return err
		}

		return PrintProgress(pb)
	},
}
