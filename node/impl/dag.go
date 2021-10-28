package impl

import (
	"context"

	"github.com/filedrive-team/filejoy/node"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
)

type DagAPI struct {
	Node *node.Node
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
