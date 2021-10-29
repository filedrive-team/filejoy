package impl

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filejoy/api"
	"github.com/filedrive-team/filejoy/node"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	ufsio "github.com/ipfs/go-unixfs/io"
)

type CommonAPI struct {
	Node *node.Node
}

func (a *CommonAPI) Add(ctx context.Context, path string) ([]cid.Cid, error) {
	// cidbuilder
	cidBuilder, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return nil, err
	}

	files := filehelper.FileWalkAsync([]string{path})
	res := make([]cid.Cid, len(files))
	for item := range files {
		fileNode, err := filehelper.BuildFileNode(item, a.Node.Dagserv, cidBuilder)
		if err != nil {
			return nil, err
		}
		res = append(res, fileNode.Cid())
	}
	return res, nil
}

func (a *CommonAPI) Get(ctx context.Context, cid cid.Cid, path string) (chan api.PBar, error) {
	dagNode, err := a.Node.Dagserv.Get(ctx, cid)
	if err != nil {
		return nil, err
	}
	fdr, err := ufsio.NewDagReader(ctx, dagNode, a.Node.Dagserv)
	if err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	pb := &pbar{
		Total: int64(fdr.Size()),
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
		_, err = io.Copy(io.MultiWriter(f, pb), fdr)
		if err != nil {
			ioerr <- err
			return
		}
		iodone <- struct{}{}
	}(iodone, ioerr)
	return out, err
}

type pbar struct {
	Total   int64
	Current int64
}

func (pb *pbar) Write(p []byte) (n int, err error) {
	l := len(p)
	pb.Current += int64(l)
	if pb.Current > pb.Total {
		pb.Current = pb.Total
	}
	return l, nil
}

func (pb *pbar) Done() bool {
	return pb.Current >= pb.Total
}
