/*
 * Copyright © 2023 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package auth

import (
	"bytes"
	"crypto/md5"
	sha "crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/clyso/chorus/pkg/dom"
	"hash"
	"io"
)

var _ io.ReadCloser = &hashReader{}

func newHashCheckReader(src io.ReadCloser, etag, sha256 string) (io.ReadCloser, error) {
	if src == nil {
		return nil, nil
	}
	var err error
	res := hashReader{}
	res.md5Src, err = hex.DecodeString(etag)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to decode etag %v", dom.ErrAuth, err)
	}
	if len(res.md5Src) != 0 {
		res.md5Hash = md5.New()
	}
	res.sha256Src, err = hex.DecodeString(sha256)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to decode sha256 %v", dom.ErrAuth, err)
	}
	if len(res.sha256Src) != 0 {
		res.sha256Hash = sha.New()
	}

	res.src = src

	return &res, nil
}

type hashReader struct {
	src        io.ReadCloser
	md5Src     []byte
	md5Hash    hash.Hash
	sha256Src  []byte
	sha256Hash hash.Hash
	readN      int
}

func (h *hashReader) Read(p []byte) (n int, err error) {
	n, err = h.src.Read(p)
	h.readN += n
	if h.md5Hash != nil {
		h.md5Hash.Write(p[:n])
	}
	if h.sha256Hash != nil {
		h.sha256Hash.Write(p[:n])
	}
	if !errors.Is(err, io.EOF) {
		return
	}

	if h.md5Hash != nil {
		sum := h.md5Hash.Sum(nil)
		if !bytes.Equal(h.md5Src, sum) {
			return n, fmt.Errorf("%w: md5 hash mismatch: expected %q, got %q", dom.ErrAuth, hex.EncodeToString(h.md5Src), hex.EncodeToString(sum))
		}
	}
	if h.sha256Hash != nil {
		sum := h.sha256Hash.Sum(nil)
		if !bytes.Equal(h.sha256Src, sum) {
			return n, fmt.Errorf("%w: sha256 hash mismatch: expected %q, got %q", dom.ErrAuth, hex.EncodeToString(h.sha256Src), hex.EncodeToString(sum))
		}
	}
	return
}

func (h *hashReader) Close() error {
	return h.src.Close()
}
