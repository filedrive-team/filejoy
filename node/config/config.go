package config

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/filedrive-team/filehelper"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

const DefaultRepoPath = "~/.filejoy"
const DefaultNodeConf = "config.json"
const lvdspath = "datastore"
const flatfspath = "blocks"
const dscfgpath = "dscluster.json"
const remotedscfgpath = "remoteds.json"
const defaultJSONRPCHost = "0.0.0.0"
const defaultJSONRPCRoot = "/rpc/v0"

type Identity struct {
	PeerID string `json:"peer_id"`
	SK     []byte `json:"sk"`
}

type JSONRPC struct {
	Host string `json:"host"`
	Port string `json:"port"`
	Root string `json:"root"`
}

type ErasureConf struct {
	ChunkServers []string `json:"chunk_servers"`
	DataShard    int      `json:"data_shard"`
	ParShard     int      `json:"par_shard"`
	ConnNum      int      `json:"conn_num"`
	Batch        int      `json:"batch"`
}

type Config struct {
	Identity      Identity `json:"identity"`
	ListenAddrs   []string `json:"listen_addrs"`
	AnnounceAddrs []string `json:"announce_addrs"`
	Bootstrappers []string `json:"bootstrappers"`

	Datastore      string      `json:"datastore"`
	Blockstore     string      `json:"blockstore"`
	DSClusterConf  string      `json:"ds_cluster_conf"`
	RemoteDSConf   string      `json:"remote_ds_conf"`
	RPC            JSONRPC     `json:"rpc"`
	Relay          bool        `json:"relay"`
	EnableRemoteDS bool        `json:"enable_remote_ds"`
	GateWayPort    uint        `json:"gateway_port"`
	Erasure        ErasureConf `json:"erasure"`
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
			ListenAddrs:   []string{fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", filehelper.RandPort())},
			Datastore:     lvdspath,
			Blockstore:    flatfspath,
			DSClusterConf: dscfgpath,
			RemoteDSConf:  remotedscfgpath,
			RPC: JSONRPC{
				Host: defaultJSONRPCHost,
				Port: filehelper.RandPort(),
				Root: defaultJSONRPCRoot,
			},
			AnnounceAddrs: make([]string, 0),
			Bootstrappers: make([]string, 0),
		}

		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		//priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader) // Qm
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

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}
	cbs, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(cbs, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
