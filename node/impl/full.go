package impl

import (
	"github.com/filedrive-team/filejoy/api"
)

type FullNodeAPI struct {
	CommonAPI
	NetAPI
	DagAPI
}

var _ api.FullNode = &FullNodeAPI{}
