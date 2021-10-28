package api

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Common interface {
	Add(context.Context, string) ([]cid.Cid, error)
	Get(context.Context, cid.Cid, string) error
}

type Net interface {
	NetConnectedness(context.Context, peer.ID) (network.Connectedness, error)
	NetPeers(context.Context) ([]peer.AddrInfo, error)
	NetConnect(context.Context, peer.AddrInfo) error
	NetAddrsListen(context.Context) (peer.AddrInfo, error)
	NetDisconnect(context.Context, peer.ID) error

	ID(context.Context) (peer.ID, error)
}

type Dag interface {
	DagStat(context.Context, cid.Cid) (*format.NodeStat, error)
}

type FullNode interface {
	Common
	Net
	Dag
}

type FullNodeClient struct {
	NetConnectedness func(context.Context, peer.ID) (network.Connectedness, error)
	NetPeers         func(context.Context) ([]peer.AddrInfo, error)
	NetConnect       func(context.Context, peer.AddrInfo) error
	NetAddrsListen   func(context.Context) (peer.AddrInfo, error)
	NetDisconnect    func(context.Context, peer.ID) error

	ID      func(context.Context) (peer.ID, error)
	DagStat func(context.Context, cid.Cid) (*format.NodeStat, error)
	Add     func(context.Context, string) ([]cid.Cid, error)
	Get     func(context.Context, cid.Cid, string) error
}

type FullNodeClientApi struct {
	Emb *FullNodeClient
}

func (a *FullNodeClientApi) ID(ctx context.Context) (peer.ID, error) {
	return a.Emb.ID(ctx)
}

func (a *FullNodeClientApi) NetConnectedness(ctx context.Context, pid peer.ID) (network.Connectedness, error) {
	return a.Emb.NetConnectedness(ctx, pid)
}

func (a *FullNodeClientApi) NetPeers(ctx context.Context) ([]peer.AddrInfo, error) {
	return a.Emb.NetPeers(ctx)
}

func (a *FullNodeClientApi) NetConnect(ctx context.Context, p peer.AddrInfo) error {
	return a.Emb.NetConnect(ctx, p)
}

func (a *FullNodeClientApi) NetAddrsListen(ctx context.Context) (peer.AddrInfo, error) {
	return a.Emb.NetAddrsListen(ctx)
}

func (a *FullNodeClientApi) NetDisconnect(ctx context.Context, p peer.ID) error {
	return a.Emb.NetDisconnect(ctx, p)
}

func (a *FullNodeClientApi) DagStat(ctx context.Context, cid cid.Cid) (*format.NodeStat, error) {
	return a.Emb.DagStat(ctx, cid)
}

func (a *FullNodeClientApi) Add(ctx context.Context, path string) ([]cid.Cid, error) {
	return a.Emb.Add(ctx, path)
}

func (a *FullNodeClientApi) Get(ctx context.Context, cid cid.Cid, path string) error {
	return a.Emb.Get(ctx, cid, path)
}
