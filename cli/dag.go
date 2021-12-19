package cli

import (
	"bufio"
	"fmt"
	"io"
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
		DagGenPieces,
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
			log.Info("usage: filejoy dag export [cid] [path]")
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

var DagGenPieces = &cli.Command{
	Name:  "gen-pieces",
	Usage: "gen pieces from cid list",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		if len(args) < 2 {
			log.Info("usage: filejoy dag gen-pieces [input-file] [path-to-filestore]")
			return nil
		}
		fileStore := args[1]
		if isDir, err := isDir(fileStore); err != nil || !isDir {
			log.Info("usage: filejoy dag gen-pieces [input-file] [path-to-filestore]")
			return err
		}
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cidListFile := args[0]
		f, err := os.Open(cidListFile)
		if err != nil {
			return err
		}
		defer f.Close()
		liner := bufio.NewReader(f)
		for {
			line, err := liner.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Println("end with input file")
				} else {
					log.Error(err)
				}
				break
			}
			arr, err := splitSSLine(line)
			if err != nil {
				return err
			}
			cid, err := cid.Decode(arr[0])
			if err != nil {
				return err
			}
			pdir, ppath := piecePath(arr[1], fileStore)
			fmt.Printf("will gen: %s\n", ppath)
			if finfo, err := os.Stat(ppath); err == nil {
				fmt.Printf("aleady has %s, size: %d, esize: %s; will ignore", ppath, finfo.Size(), arr[2])
				break
			}

			if err = os.MkdirAll(pdir, 0755); err != nil {
				return err
			}

			pb, err := api.DagExport(ctx, cid, ppath, true)
			if err != nil {
				return err
			}

			PrintProgress(pb)

		}
		return nil

	},
}

func piecePath(piececid string, filestore string) (dir string, path string) {
	dir = filestore
	pieceLen := len(piececid)
	for i := 0; i < 3; i++ {
		name := piececid[pieceLen-(i+1)*4 : pieceLen-i*4]
		dir = filepath.Join(dir, name)
	}
	path = filepath.Join(dir, piececid)
	return
}

func isDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}

	return info.IsDir(), nil
}
