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
