package util

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"net/http"
)

func WriteError(ctx context.Context, w http.ResponseWriter, err error) {
	zerolog.Ctx(ctx).Err(err).Msg("error returned")
	s3Err := mclient.ErrorResponse{}
	if errors.As(err, &s3Err) {

	} else if errors.Is(err, dom.ErrAuth) {
		s3Err = mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "AccessDenied",
			Message:    err.Error(),
			StatusCode: http.StatusForbidden,
		}
	} else if errors.Is(err, dom.ErrNotImplemented) {
		s3Err = mclient.ErrorResponse{
			Code:       "NotImplemented",
			Message:    err.Error(),
			BucketName: xctx.GetBucket(ctx),
			Key:        xctx.GetObject(ctx),
			StatusCode: http.StatusNotImplemented,
		}
	} else if errors.Is(err, dom.ErrInvalidArg) {
		s3Err = mclient.ErrorResponse{
			Code:       "InvalidArgument",
			Message:    err.Error(),
			BucketName: xctx.GetBucket(ctx),
			Key:        xctx.GetObject(ctx),
			StatusCode: http.StatusBadRequest,
		}
	} else if errors.Is(err, dom.ErrPolicy) {
		s3Err = mclient.ErrorResponse{
			Code:       "InternalErrors",
			Message:    err.Error(),
			BucketName: xctx.GetBucket(ctx),
			Key:        xctx.GetObject(ctx),
			StatusCode: http.StatusInternalServerError,
		}
	} else {
		s3Err = mclient.ErrorResponse{
			XMLName:    xml.Name{},
			Code:       "InternalError",
			Message:    "An internal error occurred.",
			StatusCode: http.StatusInternalServerError,
		}
	}
	w.WriteHeader(s3Err.StatusCode)

	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	e := xml.NewEncoder(&buf).Encode(&s3Err)
	if e != nil {
		zerolog.Ctx(ctx).Err(e).Msg("unable to marshall err resp")
		return
	}
	_, e = w.Write(buf.Bytes())
	if e != nil {
		zerolog.Ctx(ctx).Err(e).Msg("unable to write err resp")
		return
	}
}
