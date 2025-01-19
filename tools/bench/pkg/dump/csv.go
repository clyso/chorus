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

package dump

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/clyso/chorus/tools/bench/pkg/config"
)

type BenchEvent struct {
	Count int64
	Data  []string
}

func ToCSV(conf *config.Config, fileName string, header []string, ch <-chan BenchEvent) error {
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	defer w.Flush()
	if fi.Size() == 0 {
		// write header if file is empty
		err = w.Write(header)
		if err != nil {
			return err
		}
		w.Flush()
	}
	var sortWindow eventHeap
	windowSize := conf.ParallelWrites + 1
	heap.Init(&sortWindow)
	defer func() {
		// drain window into file
		for sortWindow.Len() != 0 {
			res := heap.Pop(&sortWindow).(BenchEvent)
			err = w.Write(res.Data)
			if err != nil {
				logrus.WithError(err).Errorf("unable to write benchmark to file %s", fileName)
			}
		}
	}()
	for data := range ch {
		heap.Push(&sortWindow, data)
		for sortWindow.Len() > int(windowSize) {
			res := heap.Pop(&sortWindow).(BenchEvent)
			err = w.Write(res.Data)
			if err != nil {
				return fmt.Errorf("%w: unable to write benchmark to %s", err, fileName)
			}
		}
	}
	logrus.Infof("csv: write done for file %s", fileName)
	return nil
}

func ToCSVNonBlocking(conf *config.Config, fileName string, header []string, ch <-chan BenchEvent) <-chan error {
	done := make(chan error)
	go func() {
		defer close(done)
		done <- ToCSV(conf, fileName, header, ch)
	}()
	return done
}

var _ heap.Interface = &eventHeap{}

type eventHeap []BenchEvent

func (w eventHeap) Len() int {
	return len(w)
}

func (w eventHeap) Less(i, j int) bool {
	return w[i].Count < w[j].Count
}

func (w eventHeap) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

func (w *eventHeap) Push(x any) {
	*w = append(*w, x.(BenchEvent))
}

func (w *eventHeap) Pop() any {
	val := (*w)[w.Len()-1]
	*w = (*w)[:w.Len()-1]
	return val
}
