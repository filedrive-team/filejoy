package cli

import (
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
)

var GetCmd = &cli.Command{
	Name:  "get",
	Usage: "get file by cid",
	Flags: []cli.Flag{},
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
		tp, err := homedir.Expand(args[1])
		if err != nil {
			return err
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		pb, err := api.Get(ctx, cid, tp)
		if err != nil {
			return err
		}

		var bar *progressbar.ProgressBar

		count := 0
		for item := range pb {
			if count == 0 {
				bar = progressbar.NewOptions(int(item.Total),
					progressbar.OptionEnableColorCodes(true),
					progressbar.OptionShowBytes(true),
					progressbar.OptionSetWidth(50),
					progressbar.OptionSetDescription("[cyan][reset] Writing ..."),
					progressbar.OptionSetTheme(progressbar.Theme{
						Saucer:        "[green]=[reset]",
						SaucerHead:    "[green]>[reset]",
						SaucerPadding: " ",
						BarStart:      "[",
						BarEnd:        "]",
					}),
					progressbar.OptionOnCompletion(func() {

					}),
				)
			}
			count++
			if item.Err != "" {
				log.Error(item.Err)
				return nil
			}
			bar.Set64(item.Current)
		}

		return nil
	},
}
