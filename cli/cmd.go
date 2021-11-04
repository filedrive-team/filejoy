package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filedrive-team/filejoy/api"
	"github.com/filedrive-team/filejoy/node/config"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("filejoy-cli")

var Commands = []*cli.Command{
	AddCmd,
	GetCmd,
	SyncssCmd,
	WithCategory("network", NetCmd),
	WithCategory("dag", DagCmd),
}

func WithCategory(cat string, cmd *cli.Command) *cli.Command {
	cmd.Category = strings.ToUpper(cat)
	return cmd
}

func ReqContext(cctx *cli.Context) context.Context {
	ctx, done := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 2)
	go func() {
		<-sigChan
		done()
	}()
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	return ctx
}

func GetAPI(c *cli.Context) (api.FullNode, jsonrpc.ClientCloser, error) {
	ctx := context.Background()
	repoPath := c.String("repo")
	repoPath, err := homedir.Expand(repoPath)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := config.LoadConfig(filepath.Join(repoPath, config.DefaultNodeConf))
	if err != nil {
		return nil, nil, err
	}
	apiclient := &api.FullNodeClient{}

	closer, err := jsonrpc.NewClient(ctx, fmt.Sprintf("ws://%s:%s%s", cfg.RPC.Host, cfg.RPC.Port, cfg.RPC.Root), "Filejoy", apiclient, nil)
	if err != nil {
		return nil, nil, err
	}

	fnca := &api.FullNodeClientApi{}
	fnca.Emb = apiclient
	return fnca, closer, nil
}
