package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/pierrec/lz4/v4"
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
		&cli.BoolFlag{
			Name:    "only-check",
			Aliases: []string{"oc"},
			Usage:   "",
			Value:   false,
		},
		&cli.BoolFlag{
			Name:    "save-snapshot",
			Aliases: []string{"ss"},
			Usage:   "",
			Value:   false,
		},
		&cli.StringFlag{
			Name:    "file-list",
			Aliases: []string{"fl"},
			Usage:   "sync directly from lists in the text file",
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
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		if cctx.Bool("save-snapshot") {
			pb, err := api.Get(ctx, sscid, args[0]+".txt")
			if err != nil {
				return err
			}
			if err := PrintProgress(pb); err != nil {
				return err
			}
			return nil
		}
		var totalLine, checkedLine, errLine int
		var sr *bufio.Reader
		fromFileList := cctx.String("file-list")
		if fromFileList != "" {
			f, err := os.Open(fromFileList)
			if err != nil {
				return err
			}
			defer f.Close()
			sr = bufio.NewReader(f)
		} else {
			// write snapshot to tmp file
			ssfn := filepath.Join(os.TempDir(), args[0])

			log.Infof("loading snapshot file to %s", ssfn)
			{
				pb, err := api.Get(ctx, sscid, ssfn)
				if err != nil {
					return err
				}
				if err := PrintProgress(pb); err != nil {
					return err
				}
			}

			f, err := os.Open(ssfn)
			if err != nil {
				return err
			}
			defer f.Close()
			sr = bufio.NewReader(lz4.NewReader(f))
		}

		for {
			line, err := sr.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Println("finished sync")
				} else {
					log.Error(err)
				}
				break
			}
			log.Info(line)
			arr, err := splitSSLine(line)
			if err != nil {
				return err
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

			if err = PrintProgress(pb); err != nil {
				return err
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

func splitSSLine(line string) ([]string, error) {
	err := xerrors.Errorf("unexpected line: %s", line)
	args := make([]string, 3)
	idx := strings.LastIndex(line, ",")
	if idx == -1 || idx == len(line)-1 {
		return nil, err
	}
	args[2] = line[idx+1:]
	line = line[:idx]
	idx = strings.LastIndex(line, ",")
	if idx == -1 || idx == len(line)-1 {
		return nil, err
	}
	args[1] = line[idx+1:]
	line = line[:idx]

	args[0] = line

	return args, nil
}
