package impl

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/go-padreader"
	"github.com/filedrive-team/filejoy/api"
	"github.com/filedrive-team/filejoy/node"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	gocar "github.com/ipld/go-car"
	ipldprime "github.com/ipld/go-ipld-prime"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"golang.org/x/xerrors"
)

type DagAPI struct {
	Node *node.Node
}

func (a *DagAPI) DagHas(ctx context.Context, cid cid.Cid) (bool, error) {
	return a.Node.Blockstore.Has(cid)
}

func (a *DagAPI) DagStat(ctx context.Context, cid cid.Cid) (*format.NodeStat, error) {
	dagServ := merkledag.NewDAGService(blockservice.New(a.Node.Blockstore, a.Node.Bitswap))
	dagNode, err := dagServ.Get(ctx, cid)
	if err != nil {
		return nil, err
	}
	stat, err := dagNode.Stat()
	if err != nil {
		return nil, err
	}
	return stat, nil
}

func (a *DagAPI) DagSync(ctx context.Context, cids []cid.Cid, concur int) (chan string, error) {
	var concurOption merkledag.WalkOption = merkledag.Concurrent()
	if concur > 32 {
		concurOption = merkledag.Concurrency(concur)
	}
	out := make(chan string)
	dagServ := merkledag.NewDAGService(blockservice.New(a.Node.Blockstore, a.Node.Bitswap))
	go func() {
		var err error
		for _, cc := range cids {
			err = merkledag.Walk(ctx, dagServ.GetLinks, cc, func(cid cid.Cid) bool {
				out <- cid.String()
				return true
			}, concurOption, merkledag.OnError(func(c cid.Cid, err error) error {
				if err != nil {
					out <- fmt.Sprintf("Error: %s, %s", c, err)
				}
				return nil
			}))
		}
		if err != nil {
			out <- err.Error()
		}
		close(out)
	}()

	return out, nil
}

func (a *DagAPI) DagExport(ctx context.Context, c cid.Cid, path string, pad bool) (chan api.PBar, error) {
	pr, pw := io.Pipe()

	errCh := make(chan error, 2)
	carSize := make(chan uint64)
	go func() {
		defer func() {
			if err := pw.Close(); err != nil {
				errCh <- fmt.Errorf("stream flush failed: %s", err)
			}
			close(errCh)
			close(carSize)
		}()

		selcar := gocar.NewSelectiveCar(ctx, a.Node.Blockstore, []gocar.Dag{{Root: c, Selector: allSelector()}})
		preparedCar, err := selcar.Prepare()
		if err != nil {
			errCh <- err
			return
		}
		carSize <- preparedCar.Size()
		if err := preparedCar.Dump(pw); err != nil {
			errCh <- err
		}
	}()
	cs := <-carSize
	if cs == 0 {
		return nil, xerrors.New("unable to get car size")
	}
	var total int64 = int64(cs)
	var rr io.Reader = pr

	if pad {
		r, sz := padreader.New(pr, cs)
		total = int64(sz)
		rr = r
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	pb := &pbar{
		Total: total,
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
		_, err = io.Copy(io.MultiWriter(f, pb), rr)
		if err != nil {
			ioerr <- err
			return
		}
		iodone <- struct{}{}
	}(iodone, ioerr)
	return out, err
}

func allSelector() ipldprime.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).
		Node()
}
