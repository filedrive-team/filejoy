package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/libp2p/go-libp2p-core/peer"
)

var NetCmd = &cli.Command{
	Name:  "net",
	Usage: "Manage P2P Network",
	Subcommands: []*cli.Command{
		NetPeers,
		NetConnect,
		NetListen,
		NetId,
	},
}

var NetPeers = &cli.Command{
	Name:  "peers",
	Usage: "Print peers",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)
		peers, err := api.NetPeers(ctx)
		if err != nil {
			return err
		}

		sort.Slice(peers, func(i, j int) bool {
			return strings.Compare(string(peers[i].ID), string(peers[j].ID)) > 0
		})

		for _, peer := range peers {
			fmt.Printf("%s, %s\n", peer.ID, peer.Addrs)
		}

		return nil
	},
}

var NetListen = &cli.Command{
	Name:  "listen",
	Usage: "List listen addresses",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)

		addrs, err := api.NetAddrsListen(ctx)
		if err != nil {
			return err
		}

		for _, peer := range addrs.Addrs {
			fmt.Printf("%s/p2p/%s\n", peer, addrs.ID)
		}
		return nil
	},
}

var NetConnect = &cli.Command{
	Name:      "connect",
	Usage:     "Connect to a peer",
	ArgsUsage: "[peerMultiaddr]",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)
		args := cctx.Args().Slice()

		pis := make([]*peer.AddrInfo, 0, len(args))
		for _, addr := range args {
			if maddr, err := peer.AddrInfoFromString(addr); err == nil {
				pis = append(pis, maddr)
			}
		}

		for _, pi := range pis {
			fmt.Printf("connect %s: ", pi.ID.Pretty())
			err := api.NetConnect(ctx, *pi)
			if err != nil {
				fmt.Println("failure")
				return err
			}
			fmt.Println("success")
		}

		return nil
	},
}

var NetId = &cli.Command{
	Name:  "id",
	Usage: "Get node identity",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetAPI(cctx)
		if err != nil {
			log.Errorf("%#v", err)
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		pid, err := api.ID(ctx)
		if err != nil {
			return err
		}
		fmt.Println(pid.Pretty())
		return nil
	},
}
