package impl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filejoy/api"
	"github.com/filedrive-team/filejoy/node"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	ufsio "github.com/ipfs/go-unixfs/io"
	"golang.org/x/xerrors"
)

type CommonAPI struct {
	Node *node.Node
}

func (a *CommonAPI) Add(ctx context.Context, path string) (chan api.PBar, error) {
	// cidbuilder
	cidBuilder, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return nil, err
	}
	finfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if finfo.IsDir() {
		return nil, xerrors.Errorf("%s is dir, add only works on file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	pb := &pbar{
		Total: finfo.Size(),
	}
	iodone := make(chan struct{})
	ioerr := make(chan error)
	out := make(chan api.PBar)

	go func(p chan api.PBar, iodone chan struct{}, ioerr chan error) {
		defer close(out)
		tic := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-ctx.Done():
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     ctx.Err().Error(),
				}
				return
			case <-iodone:
				return
			case <-ioerr:
				return
			case <-tic.C:
				if pb.Done() {
					out <- api.PBar{
						Total:   pb.Total,
						Current: pb.Current,
					}
				} else {
					out <- api.PBar{
						Total:   pb.Total,
						Current: pb.Current,
					}
				}
			}
		}
	}(out, iodone, ioerr)
	go func(iodone chan struct{}, ioerr chan error) {
		nd, err := filehelper.BalanceNode(io.TeeReader(f, pb), a.Node.Dagserv, cidBuilder)
		if err != nil {
			ioerr <- err
			out <- api.PBar{
				Total:   pb.Total,
				Current: pb.Current,
				Err:     err.Error(),
				Msg:     fmt.Sprintf("Add Failed: %s", err),
			}
			return
		}
		out <- api.PBar{
			Total:   pb.Total,
			Current: pb.Total,
			Msg:     fmt.Sprintf("Add Success: %s", nd.Cid()),
		}
		iodone <- struct{}{}
	}(iodone, ioerr)
	return out, err
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
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
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

	go func(p chan api.PBar, iodone chan struct{}, ioerr chan error) {
		defer close(out)
		tic := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-ctx.Done():
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     ctx.Err().Error(),
				}
				return
			case <-iodone:
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Total,
				}
				return
			case e := <-ioerr:
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     e.Error(),
				}
				return
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

func (a *CommonAPI) Add2(ctx context.Context, path string) (chan api.PBar, error) {
	// cidbuilder
	cidBuilder, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return nil, err
	}
	finfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if finfo.IsDir() {
		return nil, xerrors.Errorf("%s is dir, add only works on file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	pb := &pbar{
		Total: finfo.Size(),
	}
	iodone := make(chan struct{})
	ioerr := make(chan error)
	out := make(chan api.PBar)

	go func(p chan api.PBar, iodone chan struct{}, ioerr chan error) {
		defer close(out)
		tic := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-ctx.Done():
				out <- api.PBar{
					Total:   pb.Total,
					Current: pb.Current,
					Err:     ctx.Err().Error(),
				}
				return
			case <-iodone:
				return
			case <-ioerr:
				return
			case <-tic.C:
				if pb.Done() {
					out <- api.PBar{
						Total:   pb.Total,
						Current: pb.Current,
					}
				} else {
					out <- api.PBar{
						Total:   pb.Total,
						Current: pb.Current,
					}
				}
			}
		}
	}(out, iodone, ioerr)
	go func(iodone chan struct{}, ioerr chan error) {
		ndcid, err := BalanceNode(ctx, io.TeeReader(f, pb), a.Node.Dagserv, cidBuilder)
		if err != nil {
			ioerr <- err
			out <- api.PBar{
				Total:   pb.Total,
				Current: pb.Current,
				Err:     err.Error(),
				Msg:     fmt.Sprintf("Add Failed: %s", err),
			}
			return
		}
		out <- api.PBar{
			Total:   pb.Total,
			Current: pb.Total,
			Msg:     fmt.Sprintf("Add Success: %s", ndcid),
		}
		iodone <- struct{}{}
	}(iodone, ioerr)
	return out, err
}
