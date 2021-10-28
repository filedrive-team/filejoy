package cli

import (
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var GetCmd = &cli.Command{
	Name:  "get",
	Usage: "get file by cid",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		cid, err := cid.Decode(args[0])
		if err != nil {
			return err
		}
		tp, err := homedir.Expand(args[1])
		if err != nil {
			return err
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		return api.Get(ctx, cid, tp)
	},
}
