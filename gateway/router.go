package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	ufsio "github.com/ipfs/go-unixfs/io"
)

var log = logging.Logger("filejoy-gateway")

// InitRouter - initialize routing information
func InitRouter(ctx context.Context, dagServ format.DAGService) *gin.Engine {

	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.GET("/ipfs/:cid", func(c *gin.Context) {
		cidstr := c.Param("cid")
		log.Info("gateway search cid: %s", cidstr)
		cid, err := cid.Decode(cidstr)
		if err != nil {
			c.JSON(http.StatusBadRequest, fmt.Sprintf("invalid cid: %s", cidstr))
			return
		}
		dagNode, err := dagServ.Get(ctx, cid)
		if err != nil {
			c.JSON(http.StatusNotFound, fmt.Sprintf("could not find cid: %s", cidstr))
			return
		}
		fdr, err := ufsio.NewDagReader(ctx, dagNode, dagServ)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err.Error())
			return
		}
		var sniffbytes [512]byte
		n, _ := io.ReadFull(fdr, sniffbytes[:])
		contentType := http.DetectContentType(sniffbytes[:n])
		fdr.Seek(0, io.SeekStart)

		c.DataFromReader(http.StatusOK, int64(fdr.Size()), contentType, fdr, nil)
	})

	return r
}
