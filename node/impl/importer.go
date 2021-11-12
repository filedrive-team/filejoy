package impl

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/ipfs/go-cid"
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
				link, err := format.MakeLink(cpnd)
				if err != nil {
					return cid.Undef, err
				}

				linkList = append(linkList, &linkAndSize{
					Link:     link,
					FileSize: od.file.FileSize(),
				})

				nd = unixfs.EmptyFileNode()
				nd.SetCidBuilder(cidBuilder)
				od, err = NewFSNFromDag(nd)
				if err != nil {
					return cid.Undef, err
				}
				count = 0
			}
			if err := od.AddChild(link.Link, link.FileSize); err != nil {
				return cid.Undef, err
			}
			count++
		}
		if len(nd.Links()) > 0 {
			nnd, err := od.Commit()
			if err != nil {
				return cid.Undef, err
			}
			cpnd := nnd.Copy()
			needAdd = append(needAdd, cpnd)
			link, err := format.MakeLink(cpnd)
			if err != nil {
				return cid.Undef, err
			}
			linkList = append(linkList, &linkAndSize{
				Link:     link,
				FileSize: od.file.FileSize(),
			})
		}
		links = linkList
		linkList = make([]*linkAndSize, 0)
	}

	if len(needAdd) > 0 {
		if err := dagServ.AddMany(ctx, needAdd); err != nil {
			log.Error(err)
			return cid.Undef, err
		}
	}
	return links[0].Link.Cid, nil
}

// Todos:
//  read more bytes and parallel the dags save work
func BalanceNode(ctx context.Context, f io.Reader, fsize int64, bufDs format.DAGService, cidBuilder cid.Builder, batchReadNum int) (cid.Cid, error) {
	cker := NewBatchSplitter(f, int64(UnixfsChunkSize), batchReadNum)
	dataLinks := make([]*linkAndSize, dataLinkNum(fsize, int64(UnixfsChunkSize)))
	errchan := make(chan error)
	finishedchan := make(chan struct{})
	linkchan := make(chan IdxLink)
	go func(linkchan chan IdxLink, finishedchan chan struct{}, errchan chan error) {
		for {
			select {
			case <-ctx.Done():
				errchan <- ctx.Err()
				return
			default:
			}
			rd, err := cker.NextBytes()
			if err == io.EOF {
				finishedchan <- struct{}{}
				return
			}
			if err != nil {
				errchan <- err
				return
			}
			wg := sync.WaitGroup{}
			wg.Add(len(rd))
			for _, idxbuf := range rd {
				go func(ib *Idxbuf) {
					defer wg.Done()
					fmt.Printf("id: %d, size: %d\n", ib.Idx, len(ib.Buf))
					dag, err := NewDagWithData(ib.Buf, pb.Data_File, cidBuilder)
					if err != nil {
						errchan <- err
						return
					}
					if err = bufDs.Add(ctx, dag); err != nil {
						errchan <- err
						return
					}
					link, err := format.MakeLink(dag)
					if err != nil {
						errchan <- err
						return
					}
					linkchan <- IdxLink{
						Idx:      ib.Idx,
						Link:     link,
						FileSize: uint64(len(ib.Buf)),
					}
				}(idxbuf)
			}
			wg.Wait()
		}

	}(linkchan, finishedchan, errchan)
lab:
	for {
		select {
		case err := <-errchan:
			return cid.Undef, err
		case <-finishedchan:
			break lab
		case lk := <-linkchan:
			dataLinks[lk.Idx] = &linkAndSize{
				Link:     lk.Link,
				FileSize: lk.FileSize,
			}
		}
	}
	for _, l := range dataLinks {
		if l == nil {
			return cid.Undef, xerrors.New("unexpected data links")
		}
		//log.Infof("index: %d, bytes len: %d", i, l.FileSize)

	}
	ciid, err := buildCidByLinks(ctx, dataLinks, bufDs)
	if err != nil {
		return cid.Undef, err
	}
	return ciid, nil
}

func dataLinkNum(size, chunksize int64) int {
	if size == 0 || chunksize == 0 {
		return 0
	}
	r := float64(size) / float64(chunksize)
	return int(math.Ceil(r))
}

type IdxLink struct {
	Idx      int
	Link     *format.Link
	FileSize uint64
}

type Idxbuf struct {
	Idx int
	Buf []byte
}

type BatchSplitter struct {
	r       io.Reader
	size    uint32
	batch   uint32
	err     error
	lastidx int
}

// NewSizeSplitter returns a new size-based Splitter with the given block size.
func NewBatchSplitter(r io.Reader, size int64, batch int) *BatchSplitter {
	return &BatchSplitter{
		r:     r,
		size:  uint32(size),
		batch: uint32(batch),
	}
}

// NextBytes produces a new chunk.
func (ss *BatchSplitter) NextBytes() ([]*Idxbuf, error) {
	if ss.err != nil {
		return nil, ss.err
	}
	full := make([]byte, ss.size*ss.batch)
	n, err := io.ReadFull(ss.r, full)
	switch err {
	case io.ErrUnexpectedEOF:
		ss.err = io.EOF
		small := make([]byte, n)
		copy(small, full)
		return ss.indexedbuf(small)
	case nil:
		return ss.indexedbuf(full)
	default:
		return nil, err
	}

	// full := pool.Get(int(ss.size))
	// n, err := io.ReadFull(ss.r, full)
	// switch err {
	// case io.ErrUnexpectedEOF:
	// 	ss.err = io.EOF
	// 	small := make([]byte, n)
	// 	copy(small, full)
	// 	pool.Put(full)
	// 	return small, nil
	// case nil:
	// 	return full, nil
	// default:
	// 	pool.Put(full)
	// 	return nil, err
	// }
}

func (ss *BatchSplitter) indexedbuf(buf []byte) ([]*Idxbuf, error) {
	res := make([]*Idxbuf, 0)
	for {
		buflen := len(buf)
		if buflen <= int(ss.size) {
			res = append(res, &Idxbuf{
				Idx: ss.lastidx,
				Buf: buf,
			})
			ss.lastidx++
			break
		}
		nxtbuf := make([]byte, ss.size)
		copy(nxtbuf, buf)
		res = append(res, &Idxbuf{
			Idx: ss.lastidx,
			Buf: nxtbuf,
		})
		ss.lastidx++
		buf = buf[ss.size:]

	}
	return res, nil
}

// Reader returns the io.Reader associated to this Splitter.
func (ss *BatchSplitter) Reader() io.Reader {
	return ss.r
}
