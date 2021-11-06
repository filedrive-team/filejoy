package node

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	pbstore "github.com/filedrive-team/filehelper/blockstore"
	ncfg "github.com/filedrive-team/filejoy/node/config"
	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	dsccfg "github.com/filedrive-team/go-ds-cluster/config"
	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	dsmount "github.com/ipfs/go-datastore/mount"
	levelds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/pnet"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
)

var log = logging.Logger("filejoy-node")

// var bootstrappers = []string{
// 	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
// 	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
// 	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
// 	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
// }

type Node struct {
	Dht    *dht.IpfsDHT
	FullRT *fullrt.FullRT
	Host   host.Host

	// Set for gathering disk usage

	StorageDir string
	//Lmdb      *lmdb.Blockstore
	Datastore datastore.Batching

	Blockstore blockstore.Blockstore
	Bitswap    *bitswap.Bitswap
	Dagserv    format.DAGService

	Config *ncfg.Config
}

func Setup(ctx context.Context, cfg *ncfg.Config, repoPath string) (*Node, error) {
	peerkey, err := crypto.UnmarshalPrivateKey(cfg.Identity.SK)
	if err != nil {
		return nil, err
	}
	lds, err := levelds.NewDatastore(filepath.Join(repoPath, cfg.Datastore), nil)
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(2000, 3000, time.Minute)),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
		libp2p.DefaultTransports,
		libp2p.EnableNATService(),
	}
	if cfg.Relay {
		opts = append(opts, libp2p.DefaultEnableRelay)
		opts = append(opts, libp2p.EnableAutoRelay())
	}

	if len(cfg.AnnounceAddrs) > 0 {
		var addrs []multiaddr.Multiaddr
		for _, anna := range cfg.AnnounceAddrs {
			a, err := multiaddr.NewMultiaddr(anna)
			if err != nil {
				return nil, fmt.Errorf("failed to parse announce addr: %w", err)
			}
			addrs = append(addrs, a)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return addrs
		}))
	}
	var isPrivatenetwork bool
	// check pnet
	if kb, err := ioutil.ReadFile(filepath.Join(repoPath, "swarm.key")); err == nil {
		psk, err := pnet.DecodeV1PSK(bytes.NewReader(kb))
		if err != nil {
			return nil, fmt.Errorf("failed to configure private network: %s", err)
		}
		opts = append(opts, libp2p.PrivateNetwork(psk))
		isPrivatenetwork = true
		fmt.Println("******************** Notification: Pnet Enabled ******************")
	}
	if !isPrivatenetwork {
		opts = append(opts, libp2p.Transport(libp2pquic.NewTransport))
	}

	h, err := libp2p.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	fmt.Println("swarm listen on: ")
	for _, addr := range cfg.ListenAddrs {
		fmt.Printf("%s\n", addr)
	}

	var BootstrapPeers []peer.AddrInfo

	for _, bsp := range cfg.Bootstrappers {
		ma, err := multiaddr.NewMultiaddr(bsp)
		if err != nil {
			log.Errorf("failed to parse bootstrap address: ", err)
			continue
		}

		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Errorf("failed to create address info: ", err)
			continue
		}

		BootstrapPeers = append(BootstrapPeers, *ai)
	}

	dhtopts := fullrt.DHTOption(
		//dht.Validator(in.Validator),
		dht.Datastore(lds),
		dht.BootstrapPeers(BootstrapPeers...),
		dht.BucketSize(20),
	)

	frt, err := fullrt.NewFullRT(h, dht.DefaultPrefix, dhtopts)
	if err != nil {
		return nil, xerrors.Errorf("constructing fullrt: %w", err)
	}

	ipfsdht, err := dht.New(ctx, h, dht.Datastore(lds))
	if err != nil {
		return nil, xerrors.Errorf("constructing dht: %w", err)
	}

	dcfg, err := dsccfg.ReadConfig(filepath.Join(repoPath, cfg.DSClusterConf))
	if err != nil {
		return nil, err
	}
	var cds datastore.Datastore
	cds, err = clusterclient.NewClusterClient(context.Background(), dcfg)
	if err != nil {
		return nil, err
	}
	cds = dsmount.New([]dsmount.Mount{
		{
			Prefix:    blockstore.BlockPrefix,
			Datastore: cds,
		},
	})

	var blkst blockstore.Blockstore = blockstore.NewBlockstore(cds.(*dsmount.Datastore))
	blkst = pbstore.NewParaBlockstore(blkst, 25)

	bsnet := bsnet.NewFromIpfsHost(h, frt)

	bsctx := context.Background()
	bswap := bitswap.New(bsctx, bsnet, blkst,
		bitswap.EngineBlockstoreWorkerCount(600),
		bitswap.TaskWorkerCount(600),
		bitswap.MaxOutstandingBytesPerPeer(5<<20),
	)
	dagServ := merkledag.NewDAGService(blockservice.New(blkst, bswap))

	return &Node{
		Dht:        ipfsdht,
		FullRT:     frt,
		Host:       h,
		Blockstore: blkst,
		//Lmdb:       lmdbs,
		Datastore: lds,
		Bitswap:   bswap.(*bitswap.Bitswap),
		Dagserv:   dagServ,
		Config:    cfg,
	}, nil
}
