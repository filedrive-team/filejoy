package cli

import (
	"fmt"

	"github.com/mitchellh/go-homedir"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
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
		var bar *progressbar.ProgressBar

		count := 0
		for msg := range pb {
			if msg.Msg != "" {
				fmt.Println()
				fmt.Println(msg.Msg)
			}
			item := msg.Pb
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
				return xerrors.New(item.Err)
			}
			bar.Set64(item.Current)
		}

		return nil
	},
}
