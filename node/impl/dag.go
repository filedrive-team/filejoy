package impl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filedrive-team/filehelper/carv1"
	"github.com/filedrive-team/filejoy/api"
	"github.com/filedrive-team/filejoy/node"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	gocar "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
)

type DagAPI struct {
	Node *node.Node
}

func (a *DagAPI) DagHas(ctx context.Context, cid cid.Cid) (bool, error) {
	return a.Node.Blockstore.Has(cid)
}

func (a *DagAPI) DagStat(ctx context.Context, cid cid.Cid, timeout uint) (*format.NodeStat, error) {
	dagServ := merkledag.NewDAGService(blockservice.New(a.Node.Blockstore, a.Node.Bitswap))
	ctxx, cancel := context.WithTimeout(ctx, time.Duration(timeout*1e9))
	defer cancel()
	dagNode, err := dagServ.Get(ctxx, cid)
	if err != nil {
		return nil, err
	}
	stat, err := dagNode.Stat()
	if err != nil {
		return nil, err
	}
	return stat, nil
}

// func (a *DagAPI) DagSync(ctx context.Context, cids []cid.Cid, concur int) (chan string, error) {
// 	var concurOption merkledag.WalkOption = merkledag.Concurrent()
// 	if concur > 32 {
// 		concurOption = merkledag.Concurrency(concur)
// 	}
// 	out := make(chan string)
// 	dagServ := merkledag.NewDAGService(blockservice.New(a.Node.Blockstore, a.Node.Bitswap))
// 	go func() {
// 		var err error
// 		for _, cc := range cids {
// 			err = merkledag.Walk(ctx, dagServ.GetLinks, cc, func(cid cid.Cid) bool {
// 				out <- cid.String()
// 				return true
// 			}, concurOption, merkledag.OnError(func(c cid.Cid, err error) error {
// 				if err != nil {
// 					out <- fmt.Sprintf("Error: %s, %s", c, err)
// 				}
// 				return nil
// 			}))
// 		}
// 		if err != nil {
// 			out <- err.Error()
// 		}
// 		close(out)
// 	}()

// 	return out, nil
// }

func (a *DagAPI) DagSync(ctx context.Context, cids []cid.Cid, concur int) (chan string, error) {
	if concur <= 0 {
		concur = 1
	}
	out := make(chan string)
	doneSignal := make(chan struct{})
	cidsToLoad := make(chan cid.Cid)
	dagServ := merkledag.NewDAGService(blockservice.New(a.Node.Blockstore, a.Node.Bitswap))
	totalCids := int32(len(cids))
	numLoaded := int32(0)
	var cds sync.Once
	syncDone := func() {
		cds.Do(func() {
			close(doneSignal)
		})
	}
	go func() {
		defer close(out)
		var wg sync.WaitGroup
		wg.Add(concur)
		for i := 0; i < concur; i++ {
			go func(grid int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						out <- ctx.Err().Error()
						//log.Info("context done")
						return
					case <-doneSignal:
						return
					case cc := <-cidsToLoad:
						//log.Infof("grid: %d got %s", grid, cc)
						var nd format.Node
						var err error
						var success bool
						nd, err = dagServ.Get(ctx, cc)
						if err != nil {
							// try get dag one more time
							nd, err = dagServ.Get(ctx, cc)
							if err != nil {
								out <- fmt.Sprintf("Failed to get %s, error: %s", cc, err)
							} else {
								success = true
							}

						} else {
							success = true
						}
						if success {
							links := nd.Links()
							numlink := len(links)
							//log.Infof("new links: %d", numlink)
							if numlink > 0 {
								atomic.AddInt32(&totalCids, int32(numlink))
								go func() {
									for _, link := range links {
										cidsToLoad <- link.Cid
									}
								}()
							}
							out <- nd.Cid().String()
						}
						atomic.AddInt32(&numLoaded, 1)
						nl := atomic.LoadInt32(&numLoaded)
						tc := atomic.LoadInt32(&totalCids)
						//log.Infof("numLoaded cids: %d, total cids: %d", nl, tc)
						if nl == tc {
							syncDone()
							return
						}
					}

				}
			}(i)
		}
		wg.Wait()
	}()
	go func() {
		for _, cc := range cids {
			cidsToLoad <- cc
		}
	}()

	return out, nil
}

type onlineng struct {
	ng format.DAGService
}

func (ng *onlineng) Get(ctx context.Context, cid cid.Cid) (format.Node, error) {
	return ng.ng.Get(ctx, cid)
}

func (ng *onlineng) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	return ng.ng.GetMany(ctx, cids)
}

type offlineng struct {
	ng blockstore.Blockstore
}

func (ng *offlineng) Get(ctx context.Context, cid cid.Cid) (format.Node, error) {
	return carv1.GetNode(ctx, cid, ng.ng)
}

func (ng *offlineng) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	return nil
}

func (a *DagAPI) DagExport(ctx context.Context, c cid.Cid, path string, pad bool, batchNum int, swarm bool) (chan api.PBar, error) {
	pr, pw := io.Pipe()

	errCh := make(chan error, 2)
	var nodeGetter format.NodeGetter
	if swarm {
		nodeGetter = &onlineng{
			ng: a.Node.Dagserv,
		}
	} else {
		nodeGetter = &offlineng{
			ng: a.Node.Blockstore,
		}
	}
	go func() {
		defer func() {
			if err := pw.Close(); err != nil {
				errCh <- fmt.Errorf("stream flush failed: %s", err)
			}
			close(errCh)
		}()
		carSize, err := carv1.NewBatch(ctx, nodeGetter).Write(c, pw, batchNum)
		if err != nil {
			errCh <- err
		}
		log.Infof("cid: %s, car size: %d", c, carSize)
		if pad {
			log.Infof("pad the car ")
			if err := carv1.PadCar(pw, int64(carSize)); err != nil {
				errCh <- err
			}
		}
	}()

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	pb := &pbar{
		Total: -1,
	}
	iodone := make(chan struct{})
	ioerr := make(chan error)
	out := make(chan api.PBar)
	closeOut := sync.Once{}
	closefunc := func() {
		closeOut.Do(func() {
			close(out)
		})
	}
	go func(p chan api.PBar, iodone chan struct{}, ioerr chan error) {
		tic := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-ctx.Done():
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     ctx.Err().Error(),
				}
				closefunc()
				return
			case <-iodone:
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Total,
				}
				closefunc()
				return
			case e := <-ioerr:
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     e.Error(),
				}
				closefunc()
			case <-tic.C:
				if pb.Done() {
					out <- api.PBar{
						Total:   pb.Total,
						Current: pb.Current,
					}
					return
				}
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
				}
			}

		}
	}(out, iodone, ioerr)
	go func(iodone chan struct{}, ioerr chan error) {
		_, err = io.Copy(io.MultiWriter(f, pb), pr)
		if err != nil {
			ioerr <- err
			return
		}
		iodone <- struct{}{}
	}(iodone, ioerr)
	return out, err
}

func (a *DagAPI) DagImport(ctx context.Context, targetPath string) (chan api.PBar, error) {
	finfo, err := os.Stat(targetPath)
	if err != nil {
		return nil, err
	}

	pb := &pbar{
		Total: finfo.Size(),
	}
	iodone := make(chan struct{})
	ioerr := make(chan error)
	out := make(chan api.PBar)
	closeOut := sync.Once{}
	closefunc := func() {
		closeOut.Do(func() {
			close(out)
		})
	}
	go func(p chan api.PBar, iodone chan struct{}, ioerr chan error) {
		tic := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-ctx.Done():
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
				}
				closefunc()
				return
			case <-iodone:
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Total,
				}
				closefunc()
				return
			case e := <-ioerr:
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     e.Error(),
				}
				closefunc()
			case <-tic.C:
				if pb.Done() {
					out <- api.PBar{
						Total:   pb.Total,
						Current: pb.Current,
					}
					return
				}
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
				}
			}

		}
	}(out, iodone, ioerr)
	go func(iodone chan struct{}, ioerr chan error) {
		f, err := os.OpenFile(targetPath, os.O_RDWR, 0644)
		if err != nil {
			ioerr <- err
			return
		}
		defer f.Close()

		br := bufio.NewReader(io.TeeReader(f, pb))
		_, err = gocar.ReadHeader(br)
		if err != nil {
			ioerr <- err
			return
		}
		for {
			cid, data, err := carutil.ReadNode(br)
			if err != nil {
				if err == io.EOF {
					iodone <- struct{}{}
					break
				}
				ioerr <- err
				return
			}
			bn, err := blocks.NewBlockWithCid(data, cid)
			if err != nil {
				ioerr <- err
				return
			}
			if err = a.Node.Blockstore.Put(bn); err != nil {
				ioerr <- err
				return
			}
		}
		iodone <- struct{}{}
	}(iodone, ioerr)
	return out, err
}
