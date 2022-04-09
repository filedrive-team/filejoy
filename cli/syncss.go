package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filehelper/dataset"
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
		&cli.Int64Flag{
			Name:  "sssize",
			Value: 0, // 3TiB 3298534883328
			Usage: "split snapshot file into slice according to sssize",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		sssize := cctx.Int64("sssize")
		onlyDag := cctx.Bool("only-dag")
		onlyCheck := cctx.Bool("only-check")
		args := cctx.Args().Slice()
		if len(args) < 2 && !(onlyCheck || onlyDag || sssize > 0) {
			log.Info("usage: filejoy syncss [snapshot-cid] [target-path]")
			return nil
		}
		var err error
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		var sscidstr string
		if len(args) > 0 {
			sscidstr = args[0]
		}
		var p string

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
			sscid, err := cid.Decode(sscidstr)
			if err != nil {
				return err
			}
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
			var iorder io.Reader
			if cctx.Bool("save-snapshot") {
				cw, err := os.Getwd()
				if err != nil {
					return err
				}
				targetpath := filepath.Join(cw, args[0]+".txt")
				sf, err := os.Create(targetpath)
				if err != nil {
					return err
				}
				defer sf.Close()
				iorder = io.TeeReader(lz4.NewReader(f), sf)
			} else {
				iorder = lz4.NewReader(f)
			}
			sr = bufio.NewReader(iorder)
		}

		slice_index := 0
		slice_line_cache := make([]string, 0)
		slice_size := int64(0)
		for {
			line, err := sr.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Println("end with snapshot file")
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
			if sssize > 0 {
				// split snapshot files
				fsize, err := strconv.ParseInt(strings.TrimSuffix(arr[2], "\n"), 10, 64)
				if err != nil {
					return err
				}
				slice_size += fsize
				slice_line_cache = append(slice_line_cache, line)
				if slice_size >= sssize {
					if err := writeSlice(slice_line_cache, fmt.Sprintf("%s_%d", sscidstr, slice_index)); err != nil {
						return err
					}
					slice_size = 0
					slice_line_cache = make([]string, 0)
					slice_index++
				}
				continue
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
		if sssize > 0 && len(slice_line_cache) > 0 {
			if err := writeSlice(slice_line_cache, fmt.Sprintf("%s_%d", sscidstr, slice_index)); err != nil {
				return err
			}
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

func writeSlice(lines []string, fname string) error {
	cw, err := os.Getwd()
	if err != nil {
		return err
	}
	targetpath := filepath.Join(cw, fname)
	return os.WriteFile(targetpath, []byte(strings.Join(lines, "")), 0644)
}

var importDatasetCmd = &cli.Command{
	Name:  "import-dataset",
	Usage: "import files from the specified dataset",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "dscluster",
			Aliases: []string{"dsc"},
			Usage:   "",
		},
		&cli.IntFlag{
			Name:  "retry",
			Value: 5,
			Usage: "retry write file to datastore",
		},
		&cli.IntFlag{
			Name:  "retry-wait",
			Value: 1,
			Usage: "sleep time before a retry",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Value: 6,
			Usage: "specify batch job number",
		},
		&cli.IntFlag{
			Name:    "batch-read-num",
			Aliases: []string{"br"},
			Value:   32,
			Usage:   "specify batch read num",
		},
	},
	Action: func(c *cli.Context) (err error) {
		ctx := context.Background()
		dscluster := c.String("dscluster-cfg")
		parallel := c.Int("parallel")
		batchReadNum := c.Int("batch-read-num")

		targetPath := c.Args().First()
		targetPath, err = homedir.Expand(targetPath)
		if err != nil {
			return err
		}
		if !filehelper.ExistDir(targetPath) {
			return xerrors.Errorf("Unexpected! The path to dataset does not exist")
		}

		return dataset.Import(ctx, targetPath, dscluster, parallel, batchReadNum)
	},
}
