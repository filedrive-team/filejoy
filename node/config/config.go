package config

import (
	"crypto/rand"
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

const DefaultRepoPath = "~/.filejoy"
const DefaultNodeConf = "config.json"
const lvdspath = "datastore"
const dscfgpath = "dscluster.json"
const defaultListenAddr = "/ip4/127.0.0.1/tcp/8668"
const defaultJSONRPCPort = 3456

type Identity struct {
	PeerID string `json:"peer_id"`
	SK     []byte `json:"sk"`
}

type Config struct {
	Identity      Identity `json:"identity"`
	ListenAddrs   []string `json:"listen_addrs"`
	AnnounceAddrs []string `json:"announce_addrs"`

	Datastore     string `json:"datastore"`
	DSClusterConf string `json:"ds_cluster_conf"`
	JSONRPCPort   int    `json:"json_rpc_port"`
}

func LoadOrInitConfig(path string) (*Config, error) {
	cfg := &Config{}
	cbs, err := ioutil.ReadFile(path)
	if err == nil {
		if err = json.Unmarshal(cbs, cfg); err != nil {
			return nil, err
		}
	} else {
		if !os.IsNotExist(err) {
			return nil, err
		}
		cfg = &Config{
			ListenAddrs:   []string{defaultListenAddr},
			Datastore:     lvdspath,
			DSClusterConf: dscfgpath,
			JSONRPCPort:   defaultJSONRPCPort,
		}

		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		sk, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		cfg.Identity.PeerID = pid.Pretty()
		cfg.Identity.SK = sk
		cfgbs, err := json.MarshalIndent(cfg, "", "\t")
		if err != nil {
			return nil, err
		}
		err = ioutil.WriteFile(path, cfgbs, 0644)
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}
