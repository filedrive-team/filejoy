package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	ufsio "github.com/ipfs/go-unixfs/io"
	"golang.org/x/xerrors"
)

var log = logging.Logger("filejoy-gateway")

// InitRouter - initialize routing information
func InitRouter(ctx context.Context, dagServ format.DAGService) *gin.Engine {

	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	r.GET("/ipfs/:cid", func(c *gin.Context) {
		cidstr := c.Param("cid")
		log.Infof("gateway search cid: %s", cidstr)
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
		fsize := int64(fdr.Size())
		var sniffbytes [512]byte
		n, _ := io.ReadFull(fdr, sniffbytes[:])
		contentType := http.DetectContentType(sniffbytes[:n])
		log.Info(contentType)
		requestRange := c.Request.Header.Get("range")
		dstart, dend, drange, err := parseRange(requestRange, fsize)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err.Error())
			return
		}
		exh := make(map[string]string)
		exh["Content-Range"] = drange
		if dstart == 0 && dend+1 == fsize {
			fdr.Seek(0, io.SeekStart)
			c.DataFromReader(http.StatusOK, fsize, contentType, fdr, exh)
			return
		}
		fdr.Seek(dstart, io.SeekStart)

		c.DataFromReader(http.StatusPartialContent, dend-dstart+1, contentType, &io.LimitedReader{
			R: fdr,
			N: dend - dstart + 1,
		}, exh)
	})

	return r
}

func parseRange(ran string, total int64) (start, end int64, rh string, err error) {
	template := "bytes %d-%d/%d"
	if ran == "" || ran == "bytes=0-" {
		// load all data at once
		start = 0
		end = total - 1
		rh = fmt.Sprintf(template, start, end, total)
		return
	}
	if !strings.HasPrefix(ran, "bytes=") {
		err = xerrors.Errorf("%s bad range format", ran)
		return
	}
	ran = strings.TrimPrefix(ran, "bytes=")
	arr := strings.Split(ran, "-")
	if len(arr) != 2 {
		err = xerrors.Errorf("%s bad range format", ran)
		return
	}
	start, err = strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		return
	}
	end, err = strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		return
	}
	if start < 0 || end < 0 {
		err = xerrors.Errorf("%s unsuported range format", ran)
		return
	}
	if end > total-1 {
		end = total - 1
	}
	rh = fmt.Sprintf(template, start, end, total)
	return
}
