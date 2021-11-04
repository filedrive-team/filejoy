package cli

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/pierrec/lz4/v4"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var SyncssCmd = &cli.Command{
	Name:  "syncss",
	Usage: "sync dataset with snapshot",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:    "only-dag",
			Aliases: []string{"od"},
			Usage:   "",
			Value:   false,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		if len(args) < 2 {
			log.Info("usage: filejoy syncss [snapshot-cid] [target-path]")
			return nil
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
		sscid, err := cid.Decode(args[0])
		if err != nil {
			return err
		}
		// write snapshot to tmp file
		ssfn := filepath.Join(os.TempDir(), args[0])
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		log.Info("loading snapshot file")
		if _, err := api.Get(ctx, sscid, ssfn); err != nil {
			return err
		}
		f, err := os.Open(ssfn)
		if err != nil {
			return err
		}
		defer f.Close()
		onlyDag := cctx.Bool("only-dag")
		sr := bufio.NewReader(lz4.NewReader(f))
		for {
			line, err := sr.ReadString('\n')
			if err != nil {
				break
			}
			log.Info(line)
			arr := strings.Split(line, ",")
			if len(arr) < 3 {
				return xerrors.Errorf("unexpected line: %s", line)
			}
			fcid, err := cid.Decode(arr[1])
			if err != nil {
				return err
			}
			if onlyDag {
				info, err := api.DagSync(ctx, []cid.Cid{fcid}, 32)
				if err != nil {
					return err
				}
				for msg := range info {
					fmt.Println(msg)
				}
				continue
			}

			pb, err := api.Get(ctx, fcid, filepath.Join(p, arr[0]))
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
		}

		return nil
	},
}
