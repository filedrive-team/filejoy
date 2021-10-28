package impl

import (
	"context"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filejoy/node"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
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
