package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filedrive-team/filejoy/api"
	fcli "github.com/filedrive-team/filejoy/cli"
	"github.com/filedrive-team/filejoy/node"
	ncfg "github.com/filedrive-team/filejoy/node/config"
	"github.com/filedrive-team/filejoy/node/impl"
	"github.com/gorilla/mux"
	log "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var logging = log.Logger("filejoy")

func init() {
	log.SetLogLevelRegex("^filejoy", "INFO")
}

func main() {
	local := []*cli.Command{
		daemonCmd,
	}

	app := &cli.App{
		Name: "filejoy",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "repo",
				Usage:   "specify the repo path",
				Value:   ncfg.DefaultRepoPath,
				EnvVars: []string{"FILEJOY_PATH"},
			},
		},
		Commands: append(local, fcli.Commands...),
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}

var daemonCmd = &cli.Command{
	Name:  "daemon",
	Usage: "filejoy daemon process",
	Flags: []cli.Flag{},
	Action: func(c *cli.Context) error {
		ctx := context.Background()
		repoPath := c.String("repo")
		repoPath, err := homedir.Expand(repoPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(repoPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := ncfg.LoadOrInitConfig(path.Join(repoPath, ncfg.DefaultNodeConf))
		if err != nil {
			return err
		}
		nd, err := node.Setup(ctx, cfg, repoPath)
		if err != nil {
			return err
		}
		defer nd.Host.Close()

		// serve rpc
		var fapi api.FullNode = &impl.FullNodeAPI{
			CommonAPI: impl.CommonAPI{
				Node: nd,
			},
			NetAPI: impl.NetAPI{
				Host: nd.Host,
			},
			DagAPI: impl.DagAPI{
				Node: nd,
			},
		}
		m := mux.NewRouter()
		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Filejoy", fapi)
		m.Handle(cfg.RPC.Root, rpcServer)

		srv := &http.Server{
			Addr:    fmt.Sprintf("%s:%s", cfg.RPC.Host, cfg.RPC.Port),
			Handler: m,
		}
		go func() {
			fmt.Printf("\n\n serve json rpc at: %s:%s\n", cfg.RPC.Host, cfg.RPC.Port)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logging.Fatalf("listen: %s", err)
			}
		}()

		quit := make(chan os.Signal, 1)

		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		logging.Info("Shutdown Server ...")
		if err := srv.Shutdown(ctx); err != nil {
			logging.Fatalf("Server Shutdown: %s", err)
		}
		return nil
	},
}
