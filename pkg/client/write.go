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

	// OutOfOrder sends, for every series, a first wave of requests carrying a
	// sample at the current timestamp, followed by cfg.ReplicasPerSample further
	// waves of requests all carrying a sample timestamped oooTimestampOffset
	// behind it — a genuine out-of-order sample, sent cfg.ReplicasPerSample times
	// (so, after the first of those waves, the following ones land as duplicates
	// of an out-of-order sample rather than of the current one). Waves are sent
	// strictly in order, waiting for one to be fully acknowledged before the next
	// starts, so the intended ingestion order is guaranteed.
	OutOfOrder DuplicatedSamplesDistributionStrategy = "ooo"
)

// oooTimestampOffset is how far behind the current timestamp the OutOfOrder
// strategy's out-of-order sample is timestamped.
const oooTimestampOffset = 10 * time.Millisecond

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

	if c.cfg.DuplicatedSamplesDistributionStrategy == OutOfOrder {
		// Waves must be ingested strictly in order: the current-timestamp wave
		// first, then each out-of-order wave in turn. Otherwise the first
		// out-of-order wave might not land as a genuine first occurrence, and the
		// following ones as its duplicates.
		for _, group := range seriesGroups {
			wg := sync.WaitGroup{}
			c.sendGroup(group, &wg)
			wg.Wait()
		}
		return
	}

	// generateSineWaveSeries already grouped the series the way cfg.
	// DuplicatedSamplesDistributionStrategy needs them sent: a single group for
	// SameSeries/DifferentSeries, or one group per duplicate copy for
	// DifferentRequest. There's no ordering requirement between groups here, so
	// send them all concurrently (honoring WriteConcurrency via the gate).
	wg := sync.WaitGroup{}
	for _, group := range seriesGroups {
		c.sendGroup(group, &wg)
	}
	wg.Wait()
}

// sendGroup sends a single group of series through its own wave of batched requests
// (honoring WriteBatchSize), so a single request never mixes series across groups.
// Requests are launched as goroutines tracked by wg, which the caller must wait on.
func (c *WriteClient) sendGroup(group []*prompb.TimeSeries, wg *sync.WaitGroup) {
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
// for DifferentSeries), one group per duplicate copy for DifferentRequest, or (for
// OutOfOrder) one group at the current timestamp plus cfg.ReplicasPerSample further
// groups at an earlier, out-of-order timestamp — see generateOutOfOrderSeries.
func generateSineWaveSeries(t time.Time, cfg WriteClientConfig) [][]*prompb.TimeSeries {
	if cfg.DuplicatedSamplesDistributionStrategy == OutOfOrder {
		return generateOutOfOrderSeries(t, cfg)
	}

	extraLabels := generateExtraLabels(cfg)

	perCopy := make([][]*prompb.TimeSeries, cfg.ReplicasPerSample)
	for r := range perCopy {
		perCopy[r] = make([]*prompb.TimeSeries, 0, cfg.SeriesCount)
	}

	for seriesID := 1; seriesID <= cfg.SeriesCount; seriesID++ {
		labels := generateSeriesLabels(t, cfg, seriesID, extraLabels)

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
		return [][]*prompb.TimeSeries{flattenInSeriesOrder(perCopy)}
	}
}

// generateOutOfOrderSeries implements the OutOfOrder distribution strategy: group 0
// carries every series' sample at the current timestamp t (never duplicated), and
// groups 1..cfg.ReplicasPerSample each carry every series' sample at
// t-oooTimestampOffset — group 1 being its first occurrence, and the rest its
// duplicates. writeSeries() sends these groups strictly in order.
func generateOutOfOrderSeries(t time.Time, cfg WriteClientConfig) [][]*prompb.TimeSeries {
	extraLabels := generateExtraLabels(cfg)
	oooTs := t.Add(-oooTimestampOffset)

	out := make([][]*prompb.TimeSeries, cfg.ReplicasPerSample+1)
	for w := range out {
		out[w] = make([]*prompb.TimeSeries, 0, cfg.SeriesCount)
	}

	for seriesID := 1; seriesID <= cfg.SeriesCount; seriesID++ {
		labels := generateSeriesLabels(t, cfg, seriesID, extraLabels)

		out[0] = append(out[0], &prompb.TimeSeries{
			Labels: labels,
			Samples: []prompb.Sample{{
				Value:     generateSineWaveValue(t),
				Timestamp: t.UnixMilli(),
			}},
		})

		for r := 0; r < cfg.ReplicasPerSample; r++ {
			oooValue := generateSineWaveValue(oooTs)
			if cfg.DuplicatedSamplesValueStrategy == DifferentValue && r > 0 {
				// See the equivalent offset in generateSineWaveSeries: coarse enough to
				// survive the float64 conversion inside generateSineWaveValue.
				oooValue = generateSineWaveValue(oooTs.Add(time.Duration(r) * time.Millisecond))
			}

			out[r+1] = append(out[r+1], &prompb.TimeSeries{
				Labels: labels,
				Samples: []prompb.Sample{{
					Value:     oooValue,
					Timestamp: oooTs.UnixMilli(),
				}},
			})
		}
	}

	return out
}

// generateExtraLabels builds the cfg.ExtraLabels labels shared by every series.
func generateExtraLabels(cfg WriteClientConfig) []*prompb.Label {
	extraLabels := make([]*prompb.Label, 0, cfg.ExtraLabels)
	for j := 0; j < cfg.ExtraLabels; j++ {
		extraLabels = append(extraLabels, &prompb.Label{
			Name:  fmt.Sprintf("extraLabel%d", j),
			Value: "default",
		})
	}
	return extraLabels
}

// generateSeriesLabels builds the label set for the given series.
func generateSeriesLabels(t time.Time, cfg WriteClientConfig, seriesID int, extraLabels []*prompb.Label) []*prompb.Label {
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

	return labels
}

// flattenInSeriesOrder concatenates the copy groups returned by generateSineWaveSeries
// in series-major order: every copy of series 1, then every copy of series 2, and so
// on. This keeps a given series' duplicates contiguous, so batching by WriteBatchSize
// only ever splits between series, never in the middle of one series' duplicates.
func flattenInSeriesOrder(groups [][]*prompb.TimeSeries) []*prompb.TimeSeries {
	if len(groups) == 0 {
		return nil
	}

	numSeries := len(groups[0])
	out := make([]*prompb.TimeSeries, 0, numSeries*len(groups))

	for i := 0; i < numSeries; i++ {
		for _, group := range groups {
			out = append(out, group[i])
		}
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
