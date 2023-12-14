package bench

import (
	"encoding/csv"
	"errors"
	"github.com/clyso/chorus/tools/bench/pkg/config"
	"io"
	"os"
	"testing"
)

func TestStart(t *testing.T) {
	t.Skip()

	t.Log(Start(config.Get()))
}
func TestSparse(t *testing.T) {
	t.Skip()
	f1, err := os.OpenFile("/Users/atorubar/bench10M/bench_PUT_10.0MiB_P10_1696337052_sparse.csv", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	f2, err := os.OpenFile("/Users/atorubar/bench10M/bench_PUT_10.0MiB_P10_1696337052.csv", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f2.Close()
	writer := csv.NewWriter(f1)
	defer writer.Flush()

	reader := csv.NewReader(f2)
	res, err := reader.Read()
	if err != nil {
		panic(err)
	}
	err = writer.Write(res)
	if err != nil {
		panic(err)
	}
	i := 0
	for {
		res, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Log(err)
			continue
		}
		if i%20 == 0 {
			err = writer.Write(res)
			if err != nil {
				t.Log(err)
				continue
			}
		}
		i++
	}
}
