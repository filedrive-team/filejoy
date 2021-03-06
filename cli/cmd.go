package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
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
	"github.com/schollz/progressbar/v3"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("filejoy-cli")

var Commands = []*cli.Command{
	AddCmd,
	Add2Cmd,
	GetCmd,
	SyncssCmd,
	importDatasetCmd,
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

func PrintProgress(pb chan api.PBar) error {
	var bar *progressbar.ProgressBar

	count := 0
	for item := range pb {
		if item.Msg != "" {
			fmt.Println()
			fmt.Println(item.Msg)
		}

		if count == 0 {
			bar = progressbar.NewOptions(int(item.Total),
				progressbar.OptionEnableColorCodes(true),
				progressbar.OptionShowBytes(true),
				progressbar.OptionSetWidth(50),
				progressbar.OptionSetDescription("[cyan][reset] Writing ..."),
				progressbar.OptionSetTheme(progressbar.Theme{
					Saucer:        "[green]=[reset]",
					SaucerHead:    "[green]>[reset]",
					SaucerPadding: " ",
					BarStart:      "[",
					BarEnd:        "]",
				}),
				progressbar.OptionOnCompletion(func() {

				}),
			)
		}
		count++
		if item.Err != "" {
			return xerrors.New(item.Err)
		}
		bar.Set64(item.Current)
	}
	fmt.Println()
	return nil
}

func readLine(r *bufio.Reader, delim byte) (string, error) {
	line, err := r.ReadString(delim)
	if err != nil {
		if err == io.EOF && len(line) > 0 {
			return line, nil
		}
	}
	return line, err
}
