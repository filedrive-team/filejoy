package impl

import (
	"context"
	"fmt"

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
