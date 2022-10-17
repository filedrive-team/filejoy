package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/filecoin-project/go-padreader"
	"github.com/filedrive-team/filehelper/carv1"
	"github.com/filedrive-team/filejoy/node"
	ncfg "github.com/filedrive-team/filejoy/node/config"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
	gocar "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/mitchellh/go-homedir"
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var DagCmd = &cli.Command{
	Name:  "dag",
	Usage: "Manage dag",
	Subcommands: []*cli.Command{
		DagStat,
		DagSync,
		DagExport,
		DagImport,
		DagImport2,
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
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "timeout",
			Usage: "",
			Value: 15,
		},
	},
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

		stat, err := api.DagStat(ctx, cid, cctx.Uint("timeout"))
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
		for _, item := range cids {
			msgch, err := api.DagSync(ctx, []cid.Cid{item}, cctx.Int("concurrent"))
			if err != nil {
				return err
			}
			for msg := range msgch {
				fmt.Println(msg)
			}
		}

		return nil
	},
}

var DagImport = &cli.Command{
	Name:  "import",
	Usage: "import car file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "f",
			Usage: "",
		},
		&cli.StringFlag{
			Name:  "filestore",
			Usage: "",
		},
		&cli.BoolFlag{
			Name:  "delete-source",
			Value: false,
			Usage: "delete the car file been imported",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		cidsFilePath := cctx.String("f")
		filestorePath := cctx.String("filestore")
		deleteSource := cctx.Bool("delete-source")

		curdir, err := os.Getwd()
		if err != nil {
			return err
		}
		if cidsFilePath != "" && filestorePath != "" {
			if bs, err := ioutil.ReadFile(cidsFilePath); err == nil {
				cidlist := strings.Split(string(bs), "\n")
				for _, cidstr := range cidlist {
					cidstr = strings.TrimSpace(cidstr)
					if cidstr != "" {
						_, cidPath := piecePath(cidstr, filestorePath)
						// check if path exists
						if !fileExist(cidPath) {
							cidPath = cidPath + ".car"
						}
						if !fileExist(cidPath) {
							log.Infof("piece not exist: %s", cidPath)
							continue
						}
						args = append(args, cidPath)
					}
				}
			}
		}

		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		for _, carPath := range args {
			if !strings.HasPrefix(carPath, "/") {
				carPath = filepath.Join(curdir, carPath)
			}
			log.Infof("start to import %s", carPath)
			pb, err := api.DagImport(ctx, carPath)
			if err != nil {
				log.Error(err)
				continue
			}
			err = PrintProgress(pb)
			if err != nil {
				log.Error(err)
				continue
			}

			if deleteSource {
				if err = os.Remove(carPath); err != nil {
					log.Warn(err)
				}
			}
			log.Infof("end with import %s", carPath)
		}

		return err
	},
}

var DagImport2 = &cli.Command{
	Name:  "import2",
	Usage: "import car file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "f",
			Usage: "",
		},
		&cli.StringFlag{
			Name:  "filestore",
			Usage: "",
		},
		&cli.BoolFlag{
			Name:  "delete-source",
			Value: false,
			Usage: "delete the car file been imported",
		},
		// &cli.IntFlag{
		// 	Name:  "batch",
		// 	Value: 32,
		// 	Usage: "",
		// },
		// &cli.IntFlag{
		// 	Name:  "conn-num",
		// 	Value: 16,
		// 	Usage: "",
		// },
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()
		cidsFilePath := cctx.String("f")
		filestorePath := cctx.String("filestore")
		deleteSource := cctx.Bool("delete-source")

		curdir, err := os.Getwd()
		if err != nil {
			return err
		}
		if cidsFilePath != "" && filestorePath != "" {
			if bs, err := ioutil.ReadFile(cidsFilePath); err == nil {
				cidlist := strings.Split(string(bs), "\n")
				for _, cidstr := range cidlist {
					cidstr = strings.TrimSpace(cidstr)
					if cidstr != "" {
						_, cidPath := piecePath(cidstr, filestorePath)
						// check if path exists
						if !fileExist(cidPath) {
							cidPath = cidPath + ".car"
						}
						if !fileExist(cidPath) {
							log.Infof("piece not exist: %s", cidPath)
							continue
						}
						args = append(args, cidPath)
					}
				}
			}
		}

		repoPath := cctx.String("repo")
		repoPath, err = homedir.Expand(repoPath)
		if err != nil {
			return err
		}

		cfg, err := ncfg.LoadConfig(path.Join(repoPath, ncfg.DefaultNodeConf))
		if err != nil {
			return err
		}
		_, blkst, err := node.ConfigStorage(ctx, cfg, repoPath)
		if err != nil {
			return err
		}

		for _, carPath := range args {
			if !strings.HasPrefix(carPath, "/") {
				carPath = filepath.Join(curdir, carPath)
			}
			err = func(carPath string, blkst bstore.Blockstore, deleteSource bool) error {
				log.Infof("start to import %s", carPath)
				f, err := os.OpenFile(carPath, os.O_RDWR, 0644)
				if err != nil {
					return err
				}
				defer f.Close()
				finfo, err := f.Stat()
				if err != nil {
					return err
				}
				bar := progressbar.DefaultBytes(
					finfo.Size(),
					"importing",
				)

				br := bufio.NewReader(io.TeeReader(f, bar))
				_, err = gocar.ReadHeader(br)
				if err != nil {
					return err
				}
				for {
					cid, data, err := readDagNode(br)
					if err != nil {
						if err == io.EOF {
							log.Error(err)
						}
						break
					}
					bn, err := blocks.NewBlockWithCid(data, cid)
					if err != nil {
						return err
					}
					if err = blkst.Put(bn); err != nil {
						return err
					}
				}

				if deleteSource {
					if err = os.Remove(carPath); err != nil {
						log.Warn(err)
					}
				}
				log.Infof("end with import %s", carPath)
				return err
			}(carPath, blkst, deleteSource)
			if err != nil {
				log.Error(err)
			}
		}

		return err
	},
}

func readDagNode(r *bufio.Reader) (id cid.Cid, data []byte, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			log.Errorf("Recovered at:  %s", msg)
			err = xerrors.Errorf("%s", msg)
		}
	}()
	id, data, err = carutil.ReadNode(r)
	return
}

func fileExist(par string) bool {
	_, err := os.Stat(par)
	return err == nil
}

var DagExport = &cli.Command{
	Name:  "export",
	Usage: "export car or padded car file",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "swarm",
			Usage: "",
		},
		&cli.BoolFlag{
			Name:    "pad",
			Aliases: []string{"p"},
			Usage:   "filecoin piece pad",
		},
		&cli.IntFlag{
			Name:  "batch",
			Usage: "",
			Value: 32,
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

		pb, err := api.DagExport(ctx, cid, p, cctx.Bool("pad"), cctx.Int("batch"), cctx.Bool("swarm"))
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
		// &cli.IntFlag{
		// 	Name:  "batch",
		// 	Value: 32,
		// 	Usage: "",
		// },
		// &cli.IntFlag{
		// 	Name:  "conn-num",
		// 	Value: 16,
		// 	Usage: "",
		// },
		&cli.BoolFlag{
			Name: "flat-path",
		},
		&cli.BoolFlag{
			Name:  "pad",
			Value: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		logging.SetLogLevel("*", "Info")
		ctx := ReqContext(cctx)
		repoPath := cctx.String("repo")
		repoPath, err := homedir.Expand(repoPath)
		if err != nil {
			return err
		}

		cfg, err := ncfg.LoadConfig(path.Join(repoPath, ncfg.DefaultNodeConf))
		if err != nil {
			return err
		}

		_, blkst, err := node.ConfigStorage(ctx, cfg, repoPath)
		if err != nil {
			return err
		}

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
		var shouldPad = cctx.Bool("pad")
		var flatPath = cctx.Bool("flat-path")
		var batchNum = cctx.Int("batch")

		cidListFile := args[0]
		f, err := os.Open(cidListFile)
		if err != nil {
			return err
		}
		defer f.Close()
		liner := bufio.NewReader(f)
		for {
			line, err := readLine(liner, '\n')
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
			var pdir, ppath string
			if flatPath {
				pdir, ppath = flatPiecePath(arr[1], fileStore)
			} else {
				pdir, ppath = piecePath(arr[1], fileStore)
			}
			fmt.Printf("will gen: %s\n", ppath)
			if finfo, err := os.Stat(ppath); err == nil && finfo.Size() == expectSize {
				fmt.Printf("aleady has %s, size: %d, esize: %s; will ignore\n", ppath, finfo.Size(), arr[2])
				continue
			}

			if err = os.MkdirAll(pdir, 0755); err != nil {
				return err
			}

			// if err = writePieceV3(ctx, cid, ppath, blkst, batchNum, shouldPad); err != nil {
			// 	log.Error("%s,%s write piece failed: %s", cid, arr[1], err)
			// 	continue
			// }
			pr, pw := io.Pipe()

			errCh := make(chan error, 2)

			nodeGetter := &offlineng{
				ng: blkst,
			}

			go func() {
				defer func() {
					if err := pw.Close(); err != nil {
						errCh <- fmt.Errorf("stream flush failed: %s", err)
					}
					close(errCh)
				}()
				carSize, err := carv1.NewBatch(ctx, nodeGetter).Write(cid, pw, batchNum)
				if err != nil {
					errCh <- err
				}
				log.Infof("cid: %s, car size: %d", cid, carSize)
				if shouldPad {
					log.Infof("pad the car ")
					if err := carv1.PadCar(pw, int64(carSize)); err != nil {
						errCh <- err
					}
				}
			}()

			f, err := os.Create(ppath)
			if err != nil {
				return err
			}
			defer f.Close()

			if _, err = io.Copy(f, pr); err != nil {
				return err
			}

		}
		return nil

	},
}

type offlineng struct {
	ng bstore.Blockstore
}

func (ng *offlineng) Get(ctx context.Context, cid cid.Cid) (format.Node, error) {
	return carv1.GetNode(ctx, cid, ng.ng)
}

func (ng *offlineng) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	return nil
}

func flatPiecePath(piececid string, filestore string) (dir string, path string) {
	dir = filestore
	path = filepath.Join(dir, piececid)
	return
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

// 深度优先算法
func writePieceV3(ctx context.Context, root cid.Cid, ppath string, bs bstore.Blockstore, batchNum int, shouldPad bool) error {
	nd, err := getNode(ctx, root, bs)
	if err != nil {
		return err
	}

	f, err := os.Create(ppath)
	if err != nil {
		return err
	}
	defer f.Close()
	// write header
	if err := gocar.WriteHeader(&gocar.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}, f); err != nil {
		return err
	}

	// write data
	// write root node
	if err := carutil.LdWrite(f, nd.Cid().Bytes(), nd.RawData()); err != nil {
		return err
	}
	// set cid set to only save uniq cid to car file
	cidSet := cid.NewSet()
	cidSet.Add(nd.Cid())
	//fmt.Printf("cid: %s\n", nd.Cid())
	if err := BlockWalk(ctx, nd, bs, batchNum, func(node format.Node) error {
		if cidSet.Has(node.Cid()) {
			return nil
		}
		if err := carutil.LdWrite(f, node.Cid().Bytes(), node.RawData()); err != nil {
			return err
		}
		cidSet.Add(node.Cid())
		//fmt.Printf("cid: %s\n", node.Cid())
		return nil
	}); err != nil {
		return err
	}
	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	carSize := finfo.Size()
	log.Infof("car file size: %d", carSize)
	if shouldPad {
		pieceSize := padreader.PaddedSize(uint64(carSize))
		nr := io.LimitReader(nullReader{}, int64(pieceSize)-carSize)
		wn, err := io.Copy(f, nr)
		if err != nil {
			return err
		}
		log.Infof("padsize %d, write pad %d, piece size: %d", int64(pieceSize)-carSize, wn, pieceSize)
	}
	return nil
}

// 广度优先算法
func WritePieceV2(ctx context.Context, root cid.Cid, ppath string, bs bstore.Blockstore, batchNum int) error {
	nd, err := getNode(ctx, root, bs)
	if err != nil {
		return err
	}

	f, err := os.Create(ppath)
	if err != nil {
		return err
	}
	defer f.Close()
	// write header
	if err := gocar.WriteHeader(&gocar.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}, f); err != nil {
		return err
	}

	// write data
	// write root node
	if err := carutil.LdWrite(f, nd.Cid().Bytes(), nd.RawData()); err != nil {
		return err
	}

	unloadedLinks := nd.Links()
	if len(unloadedLinks) == 0 {
		return nil
	}

	for len(unloadedLinks) > 0 {
		loadedNodes, err := BatchLoadNode(ctx, unloadedLinks, bs, batchNum)
		if err != nil {
			return err
		}
		unloadedLinks = make([]*format.Link, 0)
		for _, nd := range loadedNodes {
			if err := carutil.LdWrite(f, nd.Cid().Bytes(), nd.RawData()); err != nil {
				return err
			}
			ndLinks := nd.Links()
			if len(ndLinks) > 0 {
				unloadedLinks = append(unloadedLinks, nd.Links()...)
			}
		}
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

func BatchLoadNode(ctx context.Context, unloadedLinks []*format.Link, bs bstore.Blockstore, batchNum int) ([]format.Node, error) {
	loadedNodes := make([]format.Node, len(unloadedLinks))
	wg := sync.WaitGroup{}
	wg.Add(len(unloadedLinks))
	bchan := make(chan struct{}, batchNum)
	errmsg := make([]string, 0)
	for i, link := range unloadedLinks {
		go func(ctx context.Context, i int, link *format.Link, bs bstore.Blockstore) {
			defer func() {
				<-bchan
				wg.Done()
			}()
			bchan <- struct{}{}
			nd, err := getNode(ctx, link.Cid, bs)
			if err != nil {
				errmsg = append(errmsg, err.Error())
			} else {
				loadedNodes[i] = nd
			}
		}(ctx, i, link, bs)
	}
	wg.Wait()
	if len(errmsg) > 0 {
		return nil, xerrors.New(strings.Join(errmsg, "\n"))
	}
	return loadedNodes, nil
}

func getNode(ctx context.Context, cid cid.Cid, bs bstore.Blockstore) (format.Node, error) {
	nd, err := bs.Get(cid)
	if err != nil {
		return nil, err
	}
	return legacy.DecodeNode(ctx, nd)
}

type nullReader struct{}

// Read writes NUL bytes into the provided byte slice.
func (nr nullReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

func BlockWalk(ctx context.Context, node format.Node, bs bstore.Blockstore, batchNum int, cb func(nd format.Node) error) error {
	links := node.Links()
	if len(links) == 0 {
		return nil
	}
	loadedNode := make([]format.Node, len(links))
	errmsg := make([]string, 0)
	var wg sync.WaitGroup
	batchchan := make(chan struct{}, batchNum)
	wg.Add(len(links))
	for i, link := range links {
		go func(ctx context.Context, i int, link *format.Link, bs bstore.Blockstore) {
			defer func() {
				<-batchchan
				wg.Done()
			}()
			batchchan <- struct{}{}
			nd, err := getNode(ctx, link.Cid, bs)
			if err != nil {
				errmsg = append(errmsg, err.Error())
			}
			loadedNode[i] = nd
		}(ctx, i, link, bs)
	}
	wg.Wait()
	if len(errmsg) > 0 {
		return xerrors.New(strings.Join(errmsg, "\n"))
	}
	for _, nd := range loadedNode {
		if err := cb(nd); err != nil {
			return err
		}
		if err := BlockWalk(ctx, nd, bs, batchNum, cb); err != nil {
			return err
		}
	}
	return nil
}
