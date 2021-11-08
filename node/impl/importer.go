package impl

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
	"golang.org/x/xerrors"

	pb "github.com/ipfs/go-unixfs/pb"
)

const UnixfsChunkSize uint64 = 1 << 20

var log = logging.Logger("filejoy-node-impl")

type linkAndSize struct {
	Link     *format.Link
	FileSize uint64
}

type FSNodeOverDag struct {
	dag  *merkledag.ProtoNode
	file *unixfs.FSNode
}

func NewFSNodeOverDag(fsNodeType pb.Data_DataType, cidBuilder cid.Builder) *FSNodeOverDag {
	node := new(FSNodeOverDag)
	node.dag = new(merkledag.ProtoNode)
	node.dag.SetCidBuilder(cidBuilder)

	node.file = unixfs.NewFSNode(fsNodeType)

	return node
}

func NewDagWithData(rd []byte, fsNodeType pb.Data_DataType, cidBuilder cid.Builder) (format.Node, error) {
	nd := NewFSNodeOverDag(fsNodeType, cidBuilder)
	nd.SetFileData(rd)
	nnd, err := nd.Commit()
	if err != nil {
		return nil, err
	}
	return nnd, nil
}

func NewFSNFromDag(nd *merkledag.ProtoNode) (*FSNodeOverDag, error) {
	mb, err := unixfs.FSNodeFromBytes(nd.Data())
	if err != nil {
		return nil, err
	}

	return &FSNodeOverDag{
		dag:  nd,
		file: mb,
	}, nil
}

func (n *FSNodeOverDag) AddChild(link *format.Link, fileSize uint64) error {
	err := n.dag.AddRawLink("", link)
	if err != nil {
		return err
	}

	n.file.AddBlockSize(fileSize)

	return nil
}

func (n *FSNodeOverDag) SetFileData(fileData []byte) {
	n.file.SetData(fileData)
}

func (n *FSNodeOverDag) Commit() (format.Node, error) {
	fileData, err := n.file.GetBytes()
	if err != nil {
		return nil, err
	}
	n.dag.SetData(fileData)

	return n.dag, nil
}

func buildCidByLinks(ctx context.Context, links []*linkAndSize, dagServ format.DAGService) (cid.Cid, error) {
	maxLinkNum := 1024
	var linkList = make([]*linkAndSize, 0)
	var needAdd = make([]format.Node, 0)

	// cidbuilder
	cidBuilder, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return cid.Undef, err
	}
	for len(links) > 1 {
		var nd *merkledag.ProtoNode = unixfs.EmptyFileNode()
		nd.SetCidBuilder(cidBuilder)
		od, err := NewFSNFromDag(nd)
		if err != nil {
			return cid.Undef, err
		}
		count := 0
		for _, link := range links {
			if count >= maxLinkNum {
				nnd, err := od.Commit()
				if err != nil {
					return cid.Undef, err
				}
				cpnd := nnd.Copy()
				needAdd = append(needAdd, cpnd)
				linkList = append(linkList, &linkAndSize{
					Link: &format.Link{
						Size: uint64(od.dag.GetPBNode().Size()),
						Cid:  od.dag.Cid(),
					},
					FileSize: od.file.FileSize(),
				})

				nd = unixfs.EmptyFileNode()
				nd.SetCidBuilder(cidBuilder)
			}
			if err := od.AddChild(link.Link, link.FileSize); err != nil {
				return cid.Undef, err
			}
			count++
		}
		if len(nd.Links()) > 0 {
			nd, err := od.Commit()
			if err != nil {
				return cid.Undef, err
			}
			cpnd := nd.Copy()
			needAdd = append(needAdd, cpnd)
			linkList = append(linkList, &linkAndSize{
				Link: &format.Link{
					Size: uint64(od.dag.GetPBNode().Size()),
					Cid:  od.dag.Cid(),
				},
				FileSize: od.file.FileSize(),
			})
		}
		links = linkList
	}

	if len(needAdd) > 0 {
		if err := dagServ.AddMany(ctx, needAdd); err != nil {
			log.Error(err)
			return cid.Undef, err
		}
	}
	return links[0].Link.Cid, nil
}

func BalanceNode(ctx context.Context, f io.Reader, bufDs format.DAGService, cidBuilder cid.Builder) (cid.Cid, error) {
	cker := chunker.NewSizeSplitter(f, int64(UnixfsChunkSize))
	dataLinks := make([]*linkAndSize, 0)
	for {
		rd, err := cker.NextBytes()
		if err == io.EOF {
			break
		}
		if err != nil {
			return cid.Undef, err
		}
		dag, err := NewDagWithData(rd, pb.Data_File, cidBuilder)
		if err != nil {
			return cid.Undef, err
		}
		link, err := format.MakeLink(dag)
		if err != nil {
			return cid.Undef, err
		}
		dataLinks = append(dataLinks, &linkAndSize{
			Link:     link,
			FileSize: uint64(len(rd)),
		})
	}
	if len(dataLinks) == 0 {
		return cid.Undef, xerrors.New("encounter empty file")
	}
	ciid, err := buildCidByLinks(ctx, dataLinks, bufDs)
	if err != nil {
		return cid.Undef, err
	}
	return ciid, nil
}
