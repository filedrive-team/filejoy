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
