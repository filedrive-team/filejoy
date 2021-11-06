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
		&cli.BoolFlag{
			Name:    "only-check",
			Aliases: []string{"oc"},
			Usage:   "",
			Value:   false,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		onlyDag := cctx.Bool("only-dag")
		onlyCheck := cctx.Bool("only-check")
		args := cctx.Args().Slice()
		if len(args) < 2 && !(onlyCheck || onlyDag) {
			log.Info("usage: filejoy syncss [snapshot-cid] [target-path]")
			return nil
		}
		var p string
		var err error
		if len(args) > 1 {
			p, err = homedir.Expand(args[1])
			if err != nil {
				return err
			}
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
		log.Infof("loading snapshot file to %s", ssfn)
		{
			pb, err := api.Get(ctx, sscid, ssfn)
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
					return xerrors.Errorf(item.Err)
				}
				bar.Set64(item.Current)
			}
		}

		f, err := os.Open(ssfn)
		if err != nil {
			return err
		}
		defer f.Close()
		var totalLine, checkedLine, errLine int
		sr := bufio.NewReader(lz4.NewReader(f))
		for {
			line, err := sr.ReadString('\n')
			if err != nil {
				log.Error(err)
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
			if onlyCheck {
				totalLine++
				if has, err := api.DagHas(ctx, fcid); has {
					checkedLine++
				} else {
					fmt.Printf("%s: %s\n", fcid, err)
					errLine++
				}
				fmt.Printf("sum: %d; has: %d; not has: %d\n", totalLine, checkedLine, errLine)
				continue
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
					return xerrors.Errorf(item.Err)
				}
				bar.Set64(item.Current)
			}
		}
		if onlyCheck {
			fmt.Printf("total: %d\n", totalLine)
			fmt.Printf("checked: %d\n", checkedLine)
			fmt.Printf("err: %d\n", errLine)
		}

		return nil
	},
}
