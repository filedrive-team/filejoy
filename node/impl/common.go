package impl

import (
	"context"
	"io"
	"os"

	"github.com/filedrive-team/filehelper"
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

func (a *CommonAPI) Get(ctx context.Context, cid cid.Cid, path string) error {
	dagNode, err := a.Node.Dagserv.Get(ctx, cid)
	if err != nil {
		return err
	}
	fdr, err := ufsio.NewDagReader(ctx, dagNode, a.Node.Dagserv)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, fdr)

	return err
}
