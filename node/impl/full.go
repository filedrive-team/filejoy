package impl

import (
	"github.com/filedrive-team/filejoy/api"
)

type FullNodeAPI struct {
	NetAPI
}

var _ api.FullNode = &FullNodeAPI{}
