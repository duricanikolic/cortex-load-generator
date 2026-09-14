package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/prompb"
)

const (
	maxErrMsgLen = 256
)

// DuplicatedSamplesValueStrategy controls the value assigned to duplicated samples.
type DuplicatedSamplesValueStrategy string

const (
	SameValue      DuplicatedSamplesValueStrategy = "same-value"
	DifferentValue DuplicatedSamplesValueStrategy = "different-value"
)

// DuplicatedSamplesDistributionStrategy controls how duplicated samples are distributed across series.
type DuplicatedSamplesDistributionStrategy string

const (
	SameSeries      DuplicatedSamplesDistributionStrategy = "same-series"
	DifferentSeries DuplicatedSamplesDistributionStrategy = "different-series"

	// DifferentRequest builds timeseries the same way as DifferentSeries (each
	// replica as its own series entry, sharing the same labels), but each of those
	// replica series is additionally sent as its own separate write request.
	DifferentRequest DuplicatedSamplesDistributionStrategy = "different-request"
)

type WriteClientConfig struct {
	// Cortex URL.
	URL url.URL

	// The tenant ID to use to push metrics to Cortex.
	UserID string

	// Number of series to generate per write request.
	SeriesCount int

	// SeriesChurnPeriod is the time period during which all series gradually churn.
	// 0 to disable churning.
	SeriesChurnPeriod time.Duration

	// Number of extra labels to generate per write request.
	ExtraLabels int

	// Number of different samples to generate in each series.
	SamplesPerSeries int

	// Number of occurrences of each sample within a single series.
	ReplicasPerSample int

	// Duplicated samples value strategy.
	DuplicatedSamplesValueStrategy DuplicatedSamplesValueStrategy

	// Duplicated samples distribution strategy.
	DuplicatedSamplesDistributionStrategy DuplicatedSamplesDistributionStrategy

	WriteInterval    time.Duration
	WriteTimeout     time.Duration
	WriteConcurrency int
	WriteBatchSize   int
}

type WriteClient struct {
	client    *http.Client
	cfg       WriteClientConfig
	writeGate *gate.Gate
	logger    log.Logger
}

func NewWriteClient(cfg WriteClientConfig, logger log.Logger) *WriteClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{userID: cfg.UserID, rt: rt}

	c := &WriteClient{
		client:    &http.Client{Transport: rt},
		cfg:       cfg,
		writeGate: gate.New(cfg.WriteConcurrency),
		logger:    logger,
	}

	return c
}

func (c *WriteClient) Start() {
	go c.run()
}

func (c *WriteClient) run() {
	c.writeSeries()

	ticker := time.NewTicker(c.cfg.WriteInterval)

	for range ticker.C {
		c.writeSeries()
	}
}

func (c *WriteClient) writeSeries() {
	ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)
	seriesGroups := generateSineWaveSeries(ts, c.cfg)

	wg := sync.WaitGroup{}

	if c.cfg.DuplicatedSamplesDistributionStrategy == DifferentRequest {
		// Group the series by "copy level": level 0 holds every series' original
		// sample, level 1 holds every series' first duplicate, and so on. Each level
		// is then sent through its own wave of batched requests (honoring
		// WriteBatchSize), so a single request never mixes series from different
		// copies.
		for _, seriesAtLevel := range transposeTimeSeries(seriesGroups) {
			for o := 0; o < len(seriesAtLevel); o += c.cfg.WriteBatchSize {
				wg.Add(1)

				go func(seriesAtLevel []*prompb.TimeSeries, o int) {
					defer wg.Done()

					// Honor the max concurrency
					ctx := context.Background()
					_ = c.writeGate.Start(ctx)
					defer c.writeGate.Done()

					end := o + c.cfg.WriteBatchSize
					if end > len(seriesAtLevel) {
						end = len(seriesAtLevel)
					}

					req := &prompb.WriteRequest{
						Timeseries: seriesAtLevel[o:end],
					}

					err := c.send(ctx, req)
					if err != nil {
						level.Error(c.logger).Log("msg", "failed to write series", "err", err)
					}
				}(seriesAtLevel, o)
			}
		}

		wg.Wait()
		return
	}

	series := flattenTimeSeries(seriesGroups)

	// Honor the batch size.
	for o := 0; o < len(series); o += c.cfg.WriteBatchSize {
		wg.Add(1)

		go func(o int) {
			defer wg.Done()

			// Honor the max concurrency
			ctx := context.Background()
			_ = c.writeGate.Start(ctx)
			defer c.writeGate.Done()

			end := o + c.cfg.WriteBatchSize
			if end > len(series) {
				end = len(series)
			}

			req := &prompb.WriteRequest{
				Timeseries: series[o:end],
			}

			err := c.send(ctx, req)
			if err != nil {
				level.Error(c.logger).Log("msg", "failed to write series", "err", err)
			}
		}(o)
	}

	wg.Wait()
}

func (c *WriteClient) send(ctx context.Context, req *prompb.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.cfg.URL.String(), bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "cortex-load-generator")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}
	return err
}

func alignTimestampToInterval(ts time.Time, interval time.Duration) time.Time {
	return time.Unix(0, (ts.UnixNano()/int64(interval))*int64(interval))
}

// generateSineWaveSeries returns, for each of the cfg.SeriesCount series, the list of
// prompb.TimeSeries entries that carry it (and its duplicates, if any) — so the outer
// slice has dimension cfg.SeriesCount, and each inner slice has dimension
// cfg.ReplicasPerSample (or 1 when cfg.DuplicatedSamplesDistributionStrategy is SameSeries).
func generateSineWaveSeries(t time.Time, cfg WriteClientConfig) [][]*prompb.TimeSeries {
	out := make([][]*prompb.TimeSeries, 0, cfg.SeriesCount)

	// Generate the extra labels.
	extraLabels := make([]*prompb.Label, 0, cfg.ExtraLabels)
	for j := 0; j < cfg.ExtraLabels; j++ {
		extraLabels = append(extraLabels, &prompb.Label{
			Name:  fmt.Sprintf("extraLabel%d", j),
			Value: "default",
		})
	}

	// Generate the samples: timestamps uniformly distributed between t and t+cfg.WriteInterval,
	// each with its own value based on its timestamp, and each replicated cfg.ReplicasPerSample
	// times. Depending on cfg.DuplicatedSamplesValueStrategy, replicas of the same sample either
	// all share the same value or each get their own distinct value.
	//
	// samples[r] holds the cfg.SamplesPerSeries samples belonging to replica r, so there are
	// cfg.ReplicasPerSample slices of cfg.SamplesPerSeries elements each.
	samples := make([][]prompb.Sample, cfg.ReplicasPerSample)
	for r := 0; r < cfg.ReplicasPerSample; r++ {
		samples[r] = make([]prompb.Sample, 0, cfg.SamplesPerSeries)
	}

	for i := 0; i < cfg.SamplesPerSeries; i++ {
		sampleTs := t.Add(cfg.WriteInterval * time.Duration(i) / time.Duration(cfg.SamplesPerSeries))

		for r := 0; r < cfg.ReplicasPerSample; r++ {
			replicaValue := generateSineWaveValue(sampleTs)
			if cfg.DuplicatedSamplesValueStrategy == DifferentValue && r > 0 {
				// A sub-millisecond offset would be rounded away by the float64 conversion
				// inside generateSineWaveValue for typical (post-1970) timestamps, producing
				// the same value as r == 0. Millisecond offsets are coarse enough to survive it.
				replicaValue = generateSineWaveValue(sampleTs.Add(time.Duration(r) * time.Millisecond))
			}

			samples[r] = append(samples[r], prompb.Sample{
				Value:     replicaValue,
				Timestamp: sampleTs.UnixMilli(),
			})
		}
	}

	for seriesID := 1; seriesID <= cfg.SeriesCount; seriesID++ {
		labels := make([]*prompb.Label, 0, 3+cfg.ExtraLabels)
		labels = append(labels, &prompb.Label{
			Name:  "__name__",
			Value: "cortex_load_generator_sine_wave",
		}, &prompb.Label{
			Name:  "wave",
			Value: strconv.Itoa(seriesID),
		})

		// Add extra labels.
		labels = append(labels, extraLabels...)

		// Add a label to simulate churning series.
		if cfg.SeriesChurnPeriod > 0 {
			// Spread churning series over the "churn period" we compute the churn ID
			// starting from the current time, shifted by the series ID. Then the value
			// is rounded so that it changes every "churn period".
			churnID := t.Add((cfg.SeriesChurnPeriod/time.Duration(cfg.SeriesCount))*time.Duration(seriesID)).Unix() / int64(cfg.SeriesChurnPeriod.Seconds())

			labels = append(labels, &prompb.Label{
				Name:  "churn",
				Value: fmt.Sprintf("%d", churnID),
			})
		}

		// Ensure labels are sorted.
		sort.Slice(labels, func(i, j int) bool {
			if labels[i].Name != labels[j].Name {
				return labels[i].Name < labels[j].Name
			}
			return labels[i].Value < labels[j].Value
		})
		out = append(out, distributeSamples(samples, labels, cfg))
	}

	return out
}

// flattenTimeSeries concatenates every series group, in order, into a single slice.
func flattenTimeSeries(groups [][]*prompb.TimeSeries) []*prompb.TimeSeries {
	total := 0
	for _, group := range groups {
		total += len(group)
	}

	out := make([]*prompb.TimeSeries, 0, total)
	for _, group := range groups {
		out = append(out, group...)
	}

	return out
}

// transposeTimeSeries turns a [series][copy] grouping into a [copy][series] one: the
// returned outer slice has one entry per copy level (e.g. level 0 is every series'
// original sample, level 1 is every series' first duplicate, and so on), and each of
// those holds one TimeSeries per input group, in the same order as groups.
func transposeTimeSeries(groups [][]*prompb.TimeSeries) [][]*prompb.TimeSeries {
	if len(groups) == 0 {
		return nil
	}

	levels := make([][]*prompb.TimeSeries, len(groups[0]))
	for l := range levels {
		levels[l] = make([]*prompb.TimeSeries, 0, len(groups))
	}

	for _, group := range groups {
		for l, series := range group {
			levels[l] = append(levels[l], series)
		}
	}

	return levels
}

func distributeSamples(samples [][]prompb.Sample, labels []*prompb.Label, cfg WriteClientConfig) []*prompb.TimeSeries {
	if cfg.DuplicatedSamplesDistributionStrategy == DifferentSeries || cfg.DuplicatedSamplesDistributionStrategy == DifferentRequest {
		out := make([]*prompb.TimeSeries, 0, cfg.ReplicasPerSample)
		// Distribute each replica to its own series entry, duplicating the labels so
		// that they still identify the same underlying series.
		for r := 0; r < cfg.ReplicasPerSample; r++ {
			out = append(out, &prompb.TimeSeries{
				Labels:  labels,
				Samples: samples[r],
			})
		}
		return out
	}

	// Put every replica of every sample into the same series, keeping samples
	// ordered by timestamp and, for a given timestamp, by replica.
	seriesSamples := make([]prompb.Sample, 0, cfg.SamplesPerSeries*cfg.ReplicasPerSample)
	for r := 0; r < cfg.ReplicasPerSample; r++ {
		seriesSamples = append(seriesSamples, samples[r]...)
	}
	return []*prompb.TimeSeries{{
		Labels:  labels,
		Samples: seriesSamples,
	}}
}

func generateSineWaveValue(t time.Time) float64 {
	// With a 15-second scrape interval this gives a ten-minute period
	period := float64(40 * (15 * time.Second))
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}
