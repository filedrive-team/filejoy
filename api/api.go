package api

import (
	"context"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Net interface {
	NetConnectedness(context.Context, peer.ID) (network.Connectedness, error)
	NetPeers(context.Context) ([]peer.AddrInfo, error)
	NetConnect(context.Context, peer.AddrInfo) error
	NetAddrsListen(context.Context) (peer.AddrInfo, error)
	NetDisconnect(context.Context, peer.ID) error

	ID(context.Context) (peer.ID, error)
}

type FullNode interface {
	Net
}

type FullNodeClient struct {
	NetConnectedness func(context.Context, peer.ID) (network.Connectedness, error)
	NetPeers         func(context.Context) ([]peer.AddrInfo, error)
	NetConnect       func(context.Context, peer.AddrInfo) error
	NetAddrsListen   func(context.Context) (peer.AddrInfo, error)
	NetDisconnect    func(context.Context, peer.ID) error

	ID func(context.Context) (peer.ID, error)
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
