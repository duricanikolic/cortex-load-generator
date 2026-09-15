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

	// generateSineWaveSeries already grouped the series the way cfg.
	// DuplicatedSamplesDistributionStrategy needs them sent: a single group for
	// SameSeries/DifferentSeries, or one group per duplicate copy for
	// DifferentRequest. Either way, each group is independently sent through its own
	// wave of batched requests (honoring WriteBatchSize), so a single request never
	// mixes series across groups.
	for _, group := range seriesGroups {
		for o := 0; o < len(group); o += c.cfg.WriteBatchSize {
			wg.Add(1)

			go func(group []*prompb.TimeSeries, o int) {
				defer wg.Done()

				// Honor the max concurrency
				ctx := context.Background()
				_ = c.writeGate.Start(ctx)
				defer c.writeGate.Done()

				end := o + c.cfg.WriteBatchSize
				if end > len(group) {
					end = len(group)
				}

				req := &prompb.WriteRequest{
					Timeseries: group[o:end],
				}

				err := c.send(ctx, req)
				if err != nil {
					level.Error(c.logger).Log("msg", "failed to write series", "err", err)
				}
			}(group, o)
		}
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

// generateSineWaveSeries returns the cfg.SeriesCount series, each replicated
// cfg.ReplicasPerSample times, already grouped the way writeSeries() needs to send
// them for cfg.DuplicatedSamplesDistributionStrategy: a single group holding every
// series (with duplicates merged into it for SameSeries, or kept as separate entries
// for DifferentSeries), or one group per duplicate copy for DifferentRequest.
func generateSineWaveSeries(t time.Time, cfg WriteClientConfig) [][]*prompb.TimeSeries {
	perCopy := make([][]*prompb.TimeSeries, cfg.ReplicasPerSample)
	for r := range perCopy {
		perCopy[r] = make([]*prompb.TimeSeries, 0, cfg.SeriesCount)
	}

	// Generate the extra labels.
	extraLabels := make([]*prompb.Label, 0, cfg.ExtraLabels)
	for j := 0; j < cfg.ExtraLabels; j++ {
		extraLabels = append(extraLabels, &prompb.Label{
			Name:  fmt.Sprintf("extraLabel%d", j),
			Value: "default",
		})
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

		// Generate the sample, replicated cfg.ReplicasPerSample times. Depending on
		// cfg.DuplicatedSamplesValueStrategy, replicas either all share the same value
		// or each get their own distinct value.
		for r := 0; r < cfg.ReplicasPerSample; r++ {
			replicaValue := generateSineWaveValue(t)
			if cfg.DuplicatedSamplesValueStrategy == DifferentValue && r > 0 {
				// A sub-millisecond offset would be rounded away by the float64 conversion
				// inside generateSineWaveValue for typical (post-1970) timestamps, producing
				// the same value as r == 0. Millisecond offsets are coarse enough to survive it.
				replicaValue = generateSineWaveValue(t.Add(time.Duration(r) * time.Millisecond))
			}

			perCopy[r] = append(perCopy[r], &prompb.TimeSeries{
				Labels: labels,
				Samples: []prompb.Sample{{
					Value:     replicaValue,
					Timestamp: t.UnixMilli(),
				}},
			})
		}
	}

	switch cfg.DuplicatedSamplesDistributionStrategy {
	case DifferentRequest:
		// One group per duplicate copy: writeSeries() will send each as its own wave
		// of requests, so a request never mixes series from different copies.
		return perCopy
	case SameSeries:
		// A single group, with every series' duplicates merged into its one entry.
		return [][]*prompb.TimeSeries{mergeCopiesPerSeries(perCopy)}
	default:
		// DifferentSeries (and the zero value): a single group, with every copy kept
		// as its own series entry.
		return [][]*prompb.TimeSeries{flattenTimeSeries(perCopy)}
	}
}

// flattenTimeSeries concatenates every copy group, in order, into a single slice.
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

// mergeCopiesPerSeries merges the copy groups returned by generateSineWaveSeries back
// into a single prompb.TimeSeries per series, concatenating all of that series' copies'
// samples together (so a series' duplicates live in the same series entry).
func mergeCopiesPerSeries(groups [][]*prompb.TimeSeries) []*prompb.TimeSeries {
	if len(groups) == 0 {
		return nil
	}

	out := make([]*prompb.TimeSeries, len(groups[0]))
	for i := range out {
		samples := make([]prompb.Sample, 0, len(groups))
		for _, copyGroup := range groups {
			samples = append(samples, copyGroup[i].Samples...)
		}

		out[i] = &prompb.TimeSeries{
			Labels:  groups[0][i].Labels,
			Samples: samples,
		}
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	// With a 15-second scrape interval this gives a ten-minute period
	period := float64(40 * (15 * time.Second))
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}
