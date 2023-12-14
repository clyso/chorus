package s3client

import (
	"fmt"
	"net/http"
)

type RequestID struct {
	ID       string
	Extended string
}

func (r RequestID) String() string {
	return r.ID + "_" + r.Extended
}

func GetRequestID(resp *http.Response) (RequestID, error) {
	res := RequestID{}
	res.ID = resp.Header.Get("x-amz-request-id")
	res.Extended = resp.Header.Get("x-amz-id-2")
	if res.ID == "" && res.Extended == "" {
		return res, fmt.Errorf("request id is not provided in s3 response")
	}
	return res, nil
}

func MustRequestID(resp *http.Response) RequestID {
	res := RequestID{}
	res.ID = resp.Header.Get("x-amz-request-id")
	res.Extended = resp.Header.Get("x-amz-id-2")
	return res
}
