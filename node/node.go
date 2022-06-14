package node

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/filedag-project/trans"
	"github.com/filedrive-team/filejoy/gateway"
	ncfg "github.com/filedrive-team/filejoy/node/config"
	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	dsccfg "github.com/filedrive-team/go-ds-cluster/config"
	dsccore "github.com/filedrive-team/go-ds-cluster/core"
	"github.com/filedrive-team/go-ds-cluster/p2p/remoteds"
	"github.com/filedrive-team/go-ds-cluster/remoteclient"
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
	circuit "github.com/libp2p/go-libp2p-circuit"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	metrics "github.com/libp2p/go-libp2p-core/metrics"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/pnet"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/fullrt"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	"github.com/multiformats/go-multiaddr"
	badgerds "github.com/textileio/go-ds-badger3"
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

	Config       *ncfg.Config
	RemotedsServ dsccore.DataNodeServer
	GatewayServ  *http.Server
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

	var ipfsdht *dht.IpfsDHT

	bwc := metrics.NewBandwidthCounter()

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.NATPortMap(),
		libp2p.ConnectionManager(connmgr.NewConnManager(2000, 3000, time.Minute)),
		libp2p.Identity(peerkey),
		libp2p.BandwidthReporter(bwc),
		libp2p.DefaultTransports,
		libp2p.EnableNATService(),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			ipfsdht, err = dht.New(ctx, h, dht.Datastore(lds))
			if err != nil {
				return nil, fmt.Errorf("constructing dht: %s", err)
			}
			return ipfsdht, nil
		}),
	}
	if cfg.Relay {
		opts = append(opts, libp2p.DefaultEnableRelay)
		opts = append(opts, libp2p.EnableRelay(circuit.OptActive, circuit.OptHop))
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
		return nil, fmt.Errorf("constructing fullrt: %s", err)
	}

	var blkst blockstore.Blockstore
	var cds datastore.Datastore
	cds, blkst, err = ConfigStorage(ctx, cfg, repoPath)

	bsnet := bsnet.NewFromIpfsHost(h, frt)

	bsctx := context.Background()
	bswap := bitswap.New(bsctx, bsnet, blkst,
		bitswap.EngineBlockstoreWorkerCount(600),
		bitswap.TaskWorkerCount(600),
		bitswap.MaxOutstandingBytesPerPeer(5<<20),
	)
	dagServ := merkledag.NewDAGService(blockservice.New(blkst, bswap))

	// serve remote datastore
	var remotedsServer dsccore.DataNodeServer
	if cfg.EnableRemoteDS && cds != nil {
		remotedsServer = remoteds.NewStoreServer(ctx, h, remoteds.PROTOCOL_V1, cds, lds, false, 180, func(token string) bool { return true }, func(token string) string { return "/ccc" })
		remotedsServer.Serve()
	}

	// serve gateway
	// currently only support simple file
	var gatewayServ *http.Server
	if cfg.GateWayPort > 0 {
		log.Info(cfg.GateWayPort)
		router := gateway.InitRouter(ctx, dagServ)

		addr := fmt.Sprintf(":%d", cfg.GateWayPort)
		maxHeaderBytes := 1 << 20

		srv := &http.Server{
			Addr:    addr,
			Handler: router,
			//ReadTimeout:    time.Duration(conf.Server.ReadTimeout * 1e9),
			//WriteTimeout:   time.Duration(conf.Server.WriteTimeout * 1e9),
			MaxHeaderBytes: maxHeaderBytes,
		}

		go func() {
			// service connections
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("listen: %s\n", err)
			}
		}()

	}

	return &Node{
		Dht:          ipfsdht,
		FullRT:       frt,
		Host:         h,
		Blockstore:   blkst,
		Datastore:    lds,
		Bitswap:      bswap.(*bitswap.Bitswap),
		Dagserv:      dagServ,
		Config:       cfg,
		RemotedsServ: remotedsServer,
		GatewayServ:  gatewayServ,
	}, nil
}

func (n *Node) Close() (err error) {
	err = n.Host.Close()
	if n.RemotedsServ != nil {
		err = n.RemotedsServ.Close()
	}
	if n.GatewayServ != nil {
		err = n.GatewayServ.Shutdown(context.TODO())
		if err == nil {
			log.Info("Gateway Server exiting")
		}
	}
	return
}

func ConfigStorage(ctx context.Context, cfg *ncfg.Config, repoPath string) (datastore.Datastore, blockstore.Blockstore, error) {
	if len(cfg.Erasure.ChunkServers) > 0 {
		blkst, err := trans.NewErasureBlockstore(ctx, cfg.Erasure.ChunkServers, cfg.Erasure.ConnNum, cfg.Erasure.DataShard, cfg.Erasure.ParShard, cfg.Erasure.Batch, "")
		if err != nil {
			return nil, nil, err
		}
		return nil, blkst, nil
	}
	return blockstoreFromDatastore(ctx, cfg, repoPath)
}

func blockstoreFromDatastore(ctx context.Context, cfg *ncfg.Config, repoPath string) (datastore.Datastore, blockstore.Blockstore, error) {
	var cds datastore.Datastore
	var err error
	dsclustercfgpath := filepath.Join(repoPath, cfg.DSClusterConf)
	remotedscfgpath := filepath.Join(repoPath, cfg.RemoteDSConf)
	_, dscerr := os.Stat(dsclustercfgpath)
	_, rdserr := os.Stat(remotedscfgpath)
	useRemoteStore := false
	if dscerr == nil {
		dcfg, err := dsccfg.ReadConfig(dsclustercfgpath)
		if err != nil {
			return nil, nil, err
		}

		cds, err = clusterclient.NewClusterClient(ctx, dcfg)
		if err != nil {
			return nil, nil, err
		}
		log.Info("use dscluster as blockstore")
	} else if rdserr == nil {
		rcfg, err := remoteclient.ReadConfig(remotedscfgpath)
		if err != nil {
			return nil, nil, err
		}
		h, err := remoteclient.HostForRemoteClient(rcfg)
		if err != nil {
			return nil, nil, err
		}
		cds, err = remoteclient.NewRemoteStore(ctx, h, rcfg.Target, rcfg.Timeout, rcfg.AccessToken)
		if err != nil {
			return nil, nil, err
		}
		useRemoteStore = true
		log.Info("use remoteds as blockstore")
	} else {
		if os.IsNotExist(dscerr) && os.IsNotExist(rdserr) {
			p := filepath.Join(repoPath, cfg.Blockstore)
			if err := os.MkdirAll(p, 0755); err != nil {
				return nil, nil, err
			}
			opts := badgerds.DefaultOptions
			cds, err = badgerds.NewDatastore(p, &opts)
			if err != nil {
				return nil, nil, err
			}
			log.Info("use badger as blockstore")
		} else {
			return nil, nil, dscerr
		}
	}
	if !useRemoteStore {
		cds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    blockstore.BlockPrefix,
				Datastore: cds,
			},
		})
	}

	var blkst blockstore.Blockstore
	if useRemoteStore {
		blkst = blockstore.NewBlockstore(cds.(*remoteclient.RemoteStore))
	} else {
		blkst = blockstore.NewBlockstore(cds.(*dsmount.Datastore))
	}
	return cds, blkst, nil
}
