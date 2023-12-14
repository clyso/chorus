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

package main

import (
	"encoding/csv"
	"github.com/wcharczuk/go-chart/v2" //exposes "chart"
	"github.com/wcharczuk/go-chart/v2/drawing"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	dataDir = "/Users/atorubar/bench10M"

	putPrefix       = "bench_PUT_"
	mainGetPrefix   = "bench_main_GET_"
	proxyGetPrefix  = "bench_proxy_GET_"
	mainListPrefix  = "bench_main_LIST_"
	proxyListPrefix = "bench_proxy_LIST_"
)

func main() {
	var (
		putFile       *os.File
		mainGetFile   *os.File
		proxyGetFile  *os.File
		mainListFile  *os.File
		proxyListFile *os.File
	)

	files, err := os.ReadDir(dataDir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		switch {
		case strings.HasPrefix(file.Name(), putPrefix):
			putFile, err = os.OpenFile(filepath.Join(dataDir, file.Name()), os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
		case strings.HasPrefix(file.Name(), mainGetPrefix):
			mainGetFile, err = os.OpenFile(filepath.Join(dataDir, file.Name()), os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
		case strings.HasPrefix(file.Name(), proxyGetPrefix):
			proxyGetFile, err = os.OpenFile(filepath.Join(dataDir, file.Name()), os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
		case strings.HasPrefix(file.Name(), mainListPrefix):
			mainListFile, err = os.OpenFile(filepath.Join(dataDir, file.Name()), os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
		case strings.HasPrefix(file.Name(), proxyListPrefix):
			proxyListFile, err = os.OpenFile(filepath.Join(dataDir, file.Name()), os.O_RDONLY, 0644)
			if err != nil {
				panic(err)
			}
		}
	}
	if putFile == nil {
		panic("putFile nil")
	}
	defer putFile.Close()
	if mainGetFile == nil {
		panic("mainGetFile nil")
	}
	defer mainGetFile.Close()
	if proxyGetFile == nil {
		panic("proxyGetFile nil")
	}
	defer proxyGetFile.Close()
	if mainListFile == nil {
		panic("mainListFile nil")
	}
	defer mainListFile.Close()
	if proxyListFile == nil {
		panic("proxyListFile nil")
	}
	defer proxyListFile.Close()

	err = renderPut(putFile)
	if err != nil {
		panic(err)
	}
	err = renderLag(putFile)
	if err != nil {
		panic(err)
	}

	err = renderGet(mainGetFile, proxyGetFile)
	if err != nil {
		panic(err)
	}

	err = renderList(mainListFile, proxyListFile)
	if err != nil {
		panic(err)
	}
}

func renderPut(f *os.File) error {
	r := csv.NewReader(f)

	res, err := r.ReadAll()
	if err != nil {
		return err
	}
	res = res[1:]
	mainSeries := chart.ContinuousSeries{
		Name:            "Put Object 10MiB",
		Style:           chart.Style{},
		XValueFormatter: chart.IntValueFormatter,
		YValueFormatter: func(v interface{}) string {
			vf := v.(float64)
			return (time.Duration(int64(vf)) * time.Microsecond).String()
		},
		XValues: make([]float64, len(res)),
		YValues: make([]float64, len(res)),
	}
	for i, data := range res {
		mainSeries.XValues[i], err = strconv.ParseFloat(data[0], 64)
		if err != nil {
			return err
		}
		mainSeries.YValues[i], err = strconv.ParseFloat(data[3], 64)
		if err != nil {
			return err
		}
	}

	linRegSeries := &chart.LinearRegressionSeries{
		InnerSeries: mainSeries,
	}

	graph := chart.Chart{
		Title:        "Put Object 10MiB",
		TitleStyle:   chart.Style{},
		ColorPalette: nil,
		Width:        1280,
		Height:       720,
		Background: chart.Style{
			Padding: chart.Box{
				Top:    80,
				Left:   80,
				Right:  80,
				Bottom: 80,
				//	IsSet:  true,
			}},
		Canvas: chart.Style{},
		XAxis: chart.XAxis{
			Ticks: []chart.Tick{
				{Value: 0.0, Label: "0"},
				{Value: 10_000.0, Label: "10k"},
				{Value: 20_000.0, Label: "20k"},
				{Value: 30_000.0, Label: "30k"},
				{Value: 40_000.0, Label: "40k"},
				{Value: 50_000.0, Label: "50k"},
				{Value: 60_000.0, Label: "60k"},
				{Value: 70_000.0, Label: "70k"},
				{Value: 80_000.0, Label: "80k"},
				{Value: 90_000.0, Label: "90k"},
				{Value: 100_000.0, Label: "100k"},
			},
		},
		YAxis:          chart.YAxis{},
		YAxisSecondary: chart.YAxis{},
		Font:           nil,
		Series: []chart.Series{
			mainSeries,
			linRegSeries,
		},
		Elements: nil,
		Log:      nil,
	}

	f, err = os.Create(filepath.Join(dataDir, "put_Object.png"))
	if err != nil {
		return err
	}
	defer f.Close()
	err = graph.Render(chart.PNG, f)
	if err != nil {
		return err
	}
	return nil
}

func renderLag(f *os.File) error {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	r := csv.NewReader(f)

	res, err := r.ReadAll()
	if err != nil {
		return err
	}
	res = res[1:]
	mainSeries := chart.ContinuousSeries{
		Name:            "Replication Lag: 10MiB, 10 Threads",
		Style:           chart.Style{},
		XValueFormatter: chart.IntValueFormatter,
		YValueFormatter: chart.IntValueFormatter,
		XValues:         make([]float64, len(res)),
		YValues:         make([]float64, len(res)),
	}
	for i, data := range res {
		mainSeries.XValues[i], err = strconv.ParseFloat(data[0], 64)
		if err != nil {
			return err
		}
		mainSeries.YValues[i], err = strconv.ParseFloat(data[6], 64)
		if err != nil {
			return err
		}
	}

	graph := chart.Chart{
		Title:        "Replication Lag: 10MiB, 10 Threads",
		TitleStyle:   chart.Style{},
		ColorPalette: nil,
		Width:        1280,
		Height:       720,
		Background: chart.Style{
			Padding: chart.Box{
				Top:    80,
				Left:   80,
				Right:  80,
				Bottom: 80,
				//	IsSet:  true,
			}},
		Canvas: chart.Style{},
		XAxis: chart.XAxis{
			Ticks: []chart.Tick{
				{Value: 0.0, Label: "0"},
				{Value: 10_000.0, Label: "10k"},
				{Value: 20_000.0, Label: "20k"},
				{Value: 30_000.0, Label: "30k"},
				{Value: 40_000.0, Label: "40k"},
				{Value: 50_000.0, Label: "50k"},
				{Value: 60_000.0, Label: "60k"},
				{Value: 70_000.0, Label: "70k"},
				{Value: 80_000.0, Label: "80k"},
				{Value: 90_000.0, Label: "90k"},
				{Value: 100_000.0, Label: "100k"},
			},
		},
		YAxis:          chart.YAxis{},
		YAxisSecondary: chart.YAxis{},
		Font:           nil,
		Series: []chart.Series{
			mainSeries,
		},
		Elements: nil,
		Log:      nil,
	}

	f, err = os.Create(filepath.Join(dataDir, "replication_lag.png"))
	if err != nil {
		return err
	}
	defer f.Close()
	err = graph.Render(chart.PNG, f)
	if err != nil {
		return err
	}
	return nil
}

func renderGet(fm, fp *os.File) error {
	rm := csv.NewReader(fm)
	rp := csv.NewReader(fp)

	resMain, err := rm.ReadAll()
	if err != nil {
		return err
	}
	resMain = resMain[1:]

	resProxy, err := rp.ReadAll()
	if err != nil {
		return err
	}
	resProxy = resProxy[1:]

	mainSeries := chart.ContinuousSeries{
		Name:  "Main",
		Style: chart.Style{
			//	StrokeColor: drawing.ColorFromAlphaMixedRGBA(26, 83, 255, 200),
		},
		XValueFormatter: chart.IntValueFormatter,
		YValueFormatter: func(v interface{}) string {
			vf := v.(float64)
			return (time.Duration(int64(vf)) * time.Microsecond).String()
		},
		XValues: make([]float64, len(resMain)),
		YValues: make([]float64, len(resMain)),
	}
	for i, data := range resMain {
		mainSeries.XValues[i], err = strconv.ParseFloat(data[0], 64)
		if err != nil {
			return err
		}
		mainSeries.YValues[i], err = strconv.ParseFloat(data[3], 64)
		if err != nil {
			return err
		}
	}

	linRegMainSeries := &chart.LinearRegressionSeries{
		InnerSeries: mainSeries,
		Name:        "Main trend",
		Style: chart.Style{
			StrokeColor: drawing.ColorBlue,
			StrokeWidth: 3.,
		},
	}

	proxySeries := chart.ContinuousSeries{
		Name:  "Proxy",
		Style: chart.Style{
			//	StrokeColor: drawing.ColorFromAlphaMixedRGBA(230, 0, 73, 200),
		},
		XValueFormatter: chart.IntValueFormatter,
		YValueFormatter: func(v interface{}) string {
			vf := v.(float64)
			return (time.Duration(int64(vf)) * time.Microsecond).String()
		},
		XValues: make([]float64, len(resMain)),
		YValues: make([]float64, len(resMain)),
	}
	for i, data := range resProxy {
		proxySeries.XValues[i], err = strconv.ParseFloat(data[0], 64)
		if err != nil {
			return err
		}
		proxySeries.YValues[i], err = strconv.ParseFloat(data[3], 64)
		if err != nil {
			return err
		}
	}

	linRegProxySeries := &chart.LinearRegressionSeries{
		InnerSeries: proxySeries,
		Name:        "Proxy trend",
		Style: chart.Style{
			StrokeColor: drawing.ColorRed,
			StrokeWidth: 3.,
		},
	}

	graph := chart.Chart{
		Title:        "GET Object 10MiB",
		TitleStyle:   chart.Style{},
		ColorPalette: nil,
		Width:        1280,
		Height:       720,
		DPI:          200.,
		Background: chart.Style{
			Padding: chart.Box{
				Top:    80,
				Left:   80,
				Right:  80,
				Bottom: 80,
				//	IsSet:  true,
			}},
		XAxis: chart.XAxis{
			Ticks: []chart.Tick{
				{Value: 0.0, Label: "0"},
				{Value: 10_000.0, Label: "10k"},
				{Value: 20_000.0, Label: "20k"},
				{Value: 30_000.0, Label: "30k"},
				{Value: 40_000.0, Label: "40k"},
				{Value: 50_000.0, Label: "50k"},
				{Value: 60_000.0, Label: "60k"},
				{Value: 70_000.0, Label: "70k"},
				{Value: 80_000.0, Label: "80k"},
				{Value: 90_000.0, Label: "90k"},
				{Value: 100_000.0, Label: "100k"},
			},
		},
		YAxis:          chart.YAxis{},
		YAxisSecondary: chart.YAxis{},
		Font:           nil,
		Series: []chart.Series{
			mainSeries,
			linRegMainSeries,
			proxySeries,
			linRegProxySeries,
		},
		Log:    nil,
		Canvas: chart.Style{},
	}
	graph.Elements = []chart.Renderable{chart.Legend(&graph)}

	f, err := os.Create(filepath.Join(dataDir, "get_Object.png"))
	if err != nil {
		return err
	}
	defer f.Close()
	err = graph.Render(chart.PNG, f)
	if err != nil {
		return err
	}
	return nil
}

func renderList(fm, fp *os.File) error {
	rm := csv.NewReader(fm)
	rp := csv.NewReader(fp)

	resMain, err := rm.ReadAll()
	if err != nil {
		return err
	}
	resMain = resMain[1:]

	resProxy, err := rp.ReadAll()
	if err != nil {
		return err
	}
	resProxy = resProxy[1:]

	mainSeries := chart.ContinuousSeries{
		Name:  "Main",
		Style: chart.Style{
			//	StrokeColor: drawing.ColorFromAlphaMixedRGBA(26, 83, 255, 200),
		},
		XValueFormatter: chart.IntValueFormatter,
		YValueFormatter: func(v interface{}) string {
			vf := v.(float64)
			return (time.Duration(int64(vf)) * time.Microsecond).String()
		},
		XValues: make([]float64, len(resMain)),
		YValues: make([]float64, len(resMain)),
	}
	for i, data := range resMain {
		mainSeries.XValues[i], err = strconv.ParseFloat(data[0], 64)
		if err != nil {
			return err
		}
		mainSeries.YValues[i], err = strconv.ParseFloat(data[4], 64)
		if err != nil {
			return err
		}
	}

	linRegMainSeries := &chart.LinearRegressionSeries{
		InnerSeries: mainSeries,
		Name:        "Main trend",
		Style: chart.Style{
			StrokeColor: drawing.ColorBlue,
			StrokeWidth: 3.,
		},
	}

	proxySeries := chart.ContinuousSeries{
		Name:  "Proxy",
		Style: chart.Style{
			//	StrokeColor: drawing.ColorFromAlphaMixedRGBA(230, 0, 73, 200),
		},
		XValueFormatter: chart.IntValueFormatter,
		YValueFormatter: func(v interface{}) string {
			vf := v.(float64)
			return (time.Duration(int64(vf)) * time.Microsecond).String()
		},
		XValues: make([]float64, len(resMain)),
		YValues: make([]float64, len(resMain)),
	}
	for i, data := range resProxy {
		proxySeries.XValues[i], err = strconv.ParseFloat(data[0], 64)
		if err != nil {
			return err
		}
		proxySeries.YValues[i], err = strconv.ParseFloat(data[4], 64)
		if err != nil {
			return err
		}
	}

	linRegProxySeries := &chart.LinearRegressionSeries{
		InnerSeries: proxySeries,
		Name:        "Proxy trend",
		Style: chart.Style{
			StrokeColor: drawing.ColorRed,
			StrokeWidth: 3.,
		},
	}

	graph := chart.Chart{
		Title:        "List 1k Object 10MiB",
		TitleStyle:   chart.Style{},
		ColorPalette: nil,
		Width:        1280,
		Height:       720,
		DPI:          200.,
		Background: chart.Style{
			Padding: chart.Box{
				Top:    80,
				Left:   80,
				Right:  80,
				Bottom: 80,
				//	IsSet:  true,
			}},
		XAxis: chart.XAxis{
			Ticks: []chart.Tick{
				{Value: 0.0, Label: "0"},
				{Value: 10_000.0, Label: "10k"},
				{Value: 20_000.0, Label: "20k"},
				{Value: 30_000.0, Label: "30k"},
				{Value: 40_000.0, Label: "40k"},
				{Value: 50_000.0, Label: "50k"},
				{Value: 60_000.0, Label: "60k"},
				{Value: 70_000.0, Label: "70k"},
				{Value: 80_000.0, Label: "80k"},
				{Value: 90_000.0, Label: "90k"},
				{Value: 100_000.0, Label: "100k"},
			},
		},
		YAxis:          chart.YAxis{},
		YAxisSecondary: chart.YAxis{},
		Font:           nil,
		Series: []chart.Series{
			mainSeries,
			linRegMainSeries,
			proxySeries,
			linRegProxySeries,
		},
		Log:    nil,
		Canvas: chart.Style{},
	}
	graph.Elements = []chart.Renderable{chart.Legend(&graph)}

	f, err := os.Create(filepath.Join(dataDir, "list_Object.png"))
	if err != nil {
		return err
	}
	defer f.Close()
	err = graph.Render(chart.PNG, f)
	if err != nil {
		return err
	}
	return nil
}
