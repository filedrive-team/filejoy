package cli

import (
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var AddCmd = &cli.Command{
	Name:  "add",
	Usage: "add files",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		tp, err := homedir.Expand(cctx.Args().First())
		if err != nil {
			return err
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		pb, err := api.Add(ctx, tp)
		if err != nil {
			return err
		}
		return PrintProgress(pb)

	},
}
