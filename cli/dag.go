package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/filecoin-project/go-padreader"
	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	dsccfg "github.com/filedrive-team/go-ds-cluster/config"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsmount "github.com/ipfs/go-datastore/mount"
	dss "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	gocar "github.com/ipld/go-car"
	ipldprime "github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
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
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "dscluster",
			Aliases: []string{"dsc"},
			Usage:   "",
		},
		&cli.IntFlag{
			Name:  "batch",
			Value: 32,
			Usage: "",
		},
	},
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
		var batchNum = cctx.Int("batch")
		var cds datastore.Datastore
		dsclustercfgpath := cctx.String("dscluster")
		dcfg, err := dsccfg.ReadConfig(dsclustercfgpath)
		if err != nil {
			return err
		}

		cds, err = clusterclient.NewClusterClient(ctx, dcfg)
		if err != nil {
			return err
		}
		cds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    bstore.BlockPrefix,
				Datastore: cds,
			},
		})
		blkst := bstore.NewBlockstore(cds.(*dsmount.Datastore))

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
			expectSize, err := strconv.ParseInt(strings.TrimSpace(arr[2]), 10, 64)
			if err != nil {
				return err
			}
			pdir, ppath := piecePath(arr[1], fileStore)
			fmt.Printf("will gen: %s\n", ppath)
			if finfo, err := os.Stat(ppath); err == nil && finfo.Size() == expectSize {
				fmt.Printf("aleady has %s, size: %d, esize: %s; will ignore", ppath, finfo.Size(), arr[2])
				break
			}

			if err = os.MkdirAll(pdir, 0755); err != nil {
				return err
			}

			if err = writePiece(ctx, cid, ppath, blkst, batchNum); err != nil {
				return err
			}

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

func writePiece(ctx context.Context, cid cid.Cid, ppath string, bs bstore.Blockstore, batchNum int) error {
	membs, err := collectDags(ctx, cid, bs, batchNum)
	if err != nil {
		return err
	}

	selcar := gocar.NewSelectiveCar(ctx, membs, []gocar.Dag{{Root: cid, Selector: allSelector()}})

	f, err := os.Create(ppath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := selcar.Write(f); err != nil {
		return err
	}
	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	carSize := finfo.Size()
	log.Infof("car file size: %d", carSize)
	pieceSize := padreader.PaddedSize(uint64(carSize))
	nr := io.LimitReader(nullReader{}, int64(pieceSize)-carSize)
	wn, err := io.Copy(f, nr)
	if err != nil {
		return err
	}
	log.Infof("padsize %d, write pad %d", int64(pieceSize)-carSize, wn)
	return nil
}

func allSelector() ipldprime.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).
		Node()
}

func collectDags(ctx context.Context, cid cid.Cid, bs bstore.Blockstore, batchNum int) (bstore.Blockstore, error) {
	dagServ := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
	memstore := bstore.NewBlockstore(dss.MutexWrap(datastore.NewMapDatastore()))
	memdag := merkledag.NewDAGService(blockservice.New(memstore, offline.Exchange(memstore)))

	if err := dagWalk(ctx, cid, dagServ, batchNum, func(nd format.Node) error {
		return memdag.Add(ctx, nd)
	}); err != nil {
		return nil, err
	}
	return memstore, nil
}

func dagWalk(ctx context.Context, cid cid.Cid, dagServ format.DAGService, batchNum int, cb func(nd format.Node) error) error {
	node, err := dagServ.Get(ctx, cid)
	if err != nil {
		return err
	}
	if err := cb(node); err != nil {
		return err
	}
	links := node.Links()
	if len(links) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	batchchan := make(chan struct{}, batchNum)
	wg.Add(len(links))
	for _, link := range links {
		go func(link *format.Link) {
			defer func() {
				<-batchchan
				wg.Done()
			}()
			batchchan <- struct{}{}
			if err := dagWalk(ctx, link.Cid, dagServ, batchNum, cb); err != nil {
				log.Error(err)
			}
		}(link)
	}
	wg.Wait()
	return nil
}

type nullReader struct{}

// Read writes NUL bytes into the provided byte slice.
func (nr nullReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}
