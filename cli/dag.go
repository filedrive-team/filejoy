package cli

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var DagCmd = &cli.Command{
	Name:  "dag",
	Usage: "Manage dag",
	Subcommands: []*cli.Command{
		DagStat,
		DagSync,
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
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		var cids = make([]cid.Cid, 0)
		for _, cidstr := range args {
			cid, err := cid.Decode(cidstr)
			if err != nil {
				return err
			}
			cids = append(cids, cid)
		}

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
