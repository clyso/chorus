/*
 * Copyright © 2026 Clyso GmbH
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

package s3

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/clyso/chorus/pkg/dom"
)

// Streaming SigV4 payloads: the body is aws-chunked and every chunk carries a
// signature chained from the seed signature of the Authorization header.
// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
const (
	StreamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	emptySHA256      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	chunkSigPrefix   = ";chunk-signature="
	trailerSigPrefix = "x-amz-trailer-signature:"
	// same limit as MinIO
	maxChunkSize = 16 << 20
)

// SigningKeyV4 derives the SigV4 signing key for the s3 service.
func SigningKeyV4(secretKey string, t time.Time, region string) []byte {
	key := hmacSHA256([]byte("AWS4"+secretKey), []byte(t.Format("20060102")))
	key = hmacSHA256(key, []byte(region))
	key = hmacSHA256(key, []byte("s3"))
	return hmacSHA256(key, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// ChunkSigner computes the chained chunk signatures of a streaming SigV4 upload.
type ChunkSigner struct {
	Key   []byte // SigningKeyV4 of the request
	Date  string // X-Amz-Date of the request
	Scope string // credential scope: date/region/s3/aws4_request
	// Prev is the seed signature, then the signature of the last chunk.
	Prev string
}

func (s *ChunkSigner) next(payloadHash string, trailer bool) string {
	var toSign string
	if trailer {
		toSign = "AWS4-HMAC-SHA256-TRAILER\n" + s.Date + "\n" + s.Scope + "\n" + s.Prev + "\n" + payloadHash
	} else {
		toSign = "AWS4-HMAC-SHA256-PAYLOAD\n" + s.Date + "\n" + s.Scope + "\n" + s.Prev + "\n" + emptySHA256 + "\n" + payloadHash
	}
	s.Prev = hex.EncodeToString(hmacSHA256(s.Key, []byte(toSign)))
	return s.Prev
}

// ResignChunks wraps an aws-chunked body: every chunk signature is checked
// against verify (if not nil) and replaced with the one computed by sign (if
// not nil). Chunk data is released only after its signature is checked.
// Signatures have a fixed length, so the body length does not change.
func ResignChunks(body io.ReadCloser, withTrailer bool, verify, sign *ChunkSigner) io.ReadCloser {
	return &chunkReader{body: body, r: bufio.NewReader(body), trailer: withTrailer, verify: verify, sign: sign}
}

type chunkReader struct {
	body         io.Closer
	r            *bufio.Reader
	trailer      bool
	verify, sign *ChunkSigner
	data         []byte
	out          bytes.Buffer
	err          error
}

func (c *chunkReader) Read(p []byte) (int, error) {
	for c.out.Len() == 0 && c.err == nil {
		c.err = c.nextChunk()
	}
	if c.out.Len() > 0 {
		return c.out.Read(p)
	}
	return 0, c.err
}

func (c *chunkReader) Close() error {
	return c.body.Close()
}

// resign checks sig and writes the signature to forward.
func (c *chunkReader) resign(sig, payloadHash string, trailer bool) error {
	if c.verify != nil {
		want := c.verify.next(payloadHash, trailer)
		if subtle.ConstantTimeCompare([]byte(want), []byte(sig)) != 1 {
			return fmt.Errorf("%w: chunk signature does not match", dom.ErrAuth)
		}
	}
	if c.sign != nil {
		sig = c.sign.next(payloadHash, trailer)
	}
	c.out.WriteString(sig)
	return nil
}

// nextChunk reads one chunk and writes it with the new signature to c.out.
// Returns io.EOF after the final chunk.
func (c *chunkReader) nextChunk() error {
	line, err := c.readLine('\n')
	if err != nil {
		return err
	}
	sizeHex, sig, ok := bytes.Cut(line, []byte(chunkSigPrefix))
	size, err := strconv.ParseInt(string(sizeHex), 16, 64)
	if !ok || err != nil || size < 0 || size > maxChunkSize {
		return fmt.Errorf("%w: malformed aws-chunked encoding", dom.ErrInvalidArg)
	}
	if cap(c.data) < int(size) {
		c.data = make([]byte, size)
	}
	c.data = c.data[:size]
	if _, err = io.ReadFull(c.r, c.data); err != nil {
		return err
	}
	hash := sha256.Sum256(c.data)
	c.out.Write(sizeHex)
	c.out.WriteString(chunkSigPrefix)
	if err = c.resign(string(sig), hex.EncodeToString(hash[:]), false); err != nil {
		return err
	}
	c.out.WriteString("\r\n")
	c.out.Write(c.data)
	if size == 0 && c.trailer {
		// the final chunk is followed by the trailer instead of CRLF
		return c.nextTrailer()
	}
	if err = c.expectCRLF(); err != nil {
		return err
	}
	c.out.WriteString("\r\n")
	if size == 0 {
		return io.EOF
	}
	return nil
}

// nextTrailer handles "<trailing headers>\r\nx-amz-trailer-signature:<sig>\r\n\r\n",
// parsed as MinIO does.
func (c *chunkReader) nextTrailer() error {
	value, err := c.readLine('\r')
	if err != nil {
		return err
	}
	if b, err := c.r.ReadByte(); err != nil || b != '\n' {
		return fmt.Errorf("%w: malformed aws-chunked trailer", dom.ErrInvalidArg)
	}
	sigLine, err := c.readLine('\n')
	if err != nil {
		return err
	}
	sig, ok := bytes.CutPrefix(sigLine, []byte(trailerSigPrefix))
	if !ok {
		return fmt.Errorf("%w: malformed aws-chunked trailer", dom.ErrInvalidArg)
	}
	c.out.Write(value)
	c.out.WriteString("\r\n" + trailerSigPrefix)
	if !bytes.HasSuffix(value, []byte{'\n'}) {
		value = append(value, '\n')
	}
	hash := sha256.Sum256(value)
	if err = c.resign(string(sig), hex.EncodeToString(hash[:]), true); err != nil {
		return err
	}
	c.out.WriteString("\r\n")
	if err = c.expectCRLF(); err != nil {
		return err
	}
	c.out.WriteString("\r\n")
	return io.EOF
}

// readLine reads up to delim, which is dropped together with the CR before
// a '\n' delim. Lines are bounded by the bufio buffer size.
func (c *chunkReader) readLine(delim byte) ([]byte, error) {
	line, err := c.r.ReadSlice(delim)
	if err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, fmt.Errorf("%w: malformed aws-chunked encoding: %w", dom.ErrInvalidArg, err)
	}
	line = line[:len(line)-1]
	if delim == '\n' {
		var ok bool
		if line, ok = bytes.CutSuffix(line, []byte{'\r'}); !ok {
			return nil, fmt.Errorf("%w: malformed aws-chunked encoding", dom.ErrInvalidArg)
		}
	}
	// ReadSlice data is overwritten by the next read
	return bytes.Clone(line), nil
}

func (c *chunkReader) expectCRLF() error {
	line, err := c.readLine('\n')
	if err != nil {
		return err
	}
	if len(line) != 0 {
		return fmt.Errorf("%w: malformed aws-chunked encoding", dom.ErrInvalidArg)
	}
	return nil
}
