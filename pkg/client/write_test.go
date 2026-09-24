package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlignTimestampToInterval(t *testing.T) {
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(30, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(31, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(39, 0), 10*time.Second))
	assert.Equal(t, time.Unix(40, 0), alignTimestampToInterval(time.Unix(40, 0), 10*time.Second))
}

func TestGenerateSineWaveSeries_WithChurningSeries(t *testing.T) {
	const (
		numSeries   = 3
		churnPeriod = time.Minute
	)

	assertGeneratedSeries := func(t *testing.T, ts time.Time, churnIDs ...string) {
		expected := make([]*prompb.TimeSeries, 0, len(churnIDs))
		for idx, churnID := range churnIDs {
			expected = append(expected, &prompb.TimeSeries{
				Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "churn", Value: churnID}, {Name: "wave", Value: strconv.Itoa(idx + 1)}},
				Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: generateSineWaveValue(ts)}},
			})
		}

		cfg := WriteClientConfig{SeriesCount: numSeries, SeriesChurnPeriod: churnPeriod, ReplicasPerSample: 1, WriteInterval: 10 * time.Second}
		actual := generateSineWaveSeries(ts, cfg)
		require.Len(t, actual, 1)
		assert.Equal(t, expected, actual[0])
	}

	ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
	require.NoError(t, err)

	assertGeneratedSeries(t, ts, "28133280", "28133280", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133280", "28133280", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133280", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133280", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133281")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133281", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133282", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133281", "28133282", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133282", "28133282", "28133282")

	ts = ts.Add(10 * time.Second)
	assertGeneratedSeries(t, ts, "28133282", "28133282", "28133282")
}

func TestGenerateSineWaveSeries_WithoutChurningSeries(t *testing.T) {
	const (
		numSeries   = 3
		churnPeriod = 0
	)

	assertGeneratedSeries := func(t *testing.T, ts time.Time) {
		expected := make([]*prompb.TimeSeries, 0, numSeries)
		for seriesID := 1; seriesID <= numSeries; seriesID++ {
			expected = append(expected, &prompb.TimeSeries{
				Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "wave", Value: strconv.Itoa(seriesID)}},
				Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: generateSineWaveValue(ts)}},
			})
		}

		cfg := WriteClientConfig{SeriesCount: numSeries, SeriesChurnPeriod: churnPeriod, ReplicasPerSample: 1, WriteInterval: 10 * time.Second}
		actual := generateSineWaveSeries(ts, cfg)
		require.Len(t, actual, 1)
		assert.Equal(t, expected, actual[0])
	}

	ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		assertGeneratedSeries(t, ts)
		ts = ts.Add(10 * time.Second)
	}
}

func TestGenerateSineWaveSeries_WithReplicas(t *testing.T) {
	const (
		numSeries     = 3
		churnPeriod   = 0
		writeInterval = 10 * time.Second
	)

	testCases := map[string]struct {
		replicasPerSample    int
		valueStrategy        DuplicatedSamplesValueStrategy
		distributionStrategy DuplicatedSamplesDistributionStrategy
	}{
		"no replicas, same series": {
			replicasPerSample:    1,
			valueStrategy:        SameValue,
			distributionStrategy: SameSeries,
		},
		"no replicas, different series": {
			replicasPerSample:    1,
			valueStrategy:        SameValue,
			distributionStrategy: DifferentSeries,
		},
		"no replicas, different request": {
			replicasPerSample:    1,
			valueStrategy:        SameValue,
			distributionStrategy: DifferentRequest,
		},
		"multiple replicas, same value, same series": {
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: SameSeries,
		},
		"multiple replicas, different value, same series": {
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: SameSeries,
		},
		"multiple replicas, same value, different series": {
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: DifferentSeries,
		},
		"multiple replicas, different value, different series": {
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: DifferentSeries,
		},
		"multiple replicas, same value, different request": {
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: DifferentRequest,
		},
		"multiple replicas, different value, different request": {
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: DifferentRequest,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assertGeneratedSeries := func(t *testing.T, ts time.Time) {
				value := generateSineWaveValue(ts)

				// perCopy[r][i] is series i+1's r-th copy — the raw building block,
				// before it gets grouped per cfg.DuplicatedSamplesDistributionStrategy.
				perCopy := make([][]*prompb.TimeSeries, tc.replicasPerSample)
				for r := 0; r < tc.replicasPerSample; r++ {
					replicaValue := value
					if tc.valueStrategy == DifferentValue && r > 0 {
						replicaValue = generateSineWaveValue(ts.Add(time.Duration(r) * time.Millisecond))
					}

					perCopy[r] = make([]*prompb.TimeSeries, 0, numSeries)
					for seriesID := 1; seriesID <= numSeries; seriesID++ {
						perCopy[r] = append(perCopy[r], &prompb.TimeSeries{
							Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "wave", Value: strconv.Itoa(seriesID)}},
							Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: replicaValue}},
						})
					}
				}

				var expected [][]*prompb.TimeSeries
				switch tc.distributionStrategy {
				case DifferentRequest:
					expected = perCopy
				case SameSeries:
					merged := make([]*prompb.TimeSeries, numSeries)
					for i := 0; i < numSeries; i++ {
						samples := make([]prompb.Sample, 0, tc.replicasPerSample)
						for r := 0; r < tc.replicasPerSample; r++ {
							samples = append(samples, perCopy[r][i].Samples...)
						}
						merged[i] = &prompb.TimeSeries{Labels: perCopy[0][i].Labels, Samples: samples}
					}
					expected = [][]*prompb.TimeSeries{merged}
				default: // DifferentSeries
					// Series-major: every copy of series 1, then every copy of series 2,
					// etc., so a series' duplicates stay contiguous.
					flat := make([]*prompb.TimeSeries, 0, tc.replicasPerSample*numSeries)
					for i := 0; i < numSeries; i++ {
						for r := 0; r < tc.replicasPerSample; r++ {
							flat = append(flat, perCopy[r][i])
						}
					}
					expected = [][]*prompb.TimeSeries{flat}
				}

				cfg := WriteClientConfig{
					SeriesCount:                           numSeries,
					SeriesChurnPeriod:                     churnPeriod,
					ReplicasPerSample:                     tc.replicasPerSample,
					DuplicatedSamplesValueStrategy:        tc.valueStrategy,
					DuplicatedSamplesDistributionStrategy: tc.distributionStrategy,
					WriteInterval:                         writeInterval,
				}
				assert.Equal(t, expected, generateSineWaveSeries(ts, cfg))
			}

			ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				assertGeneratedSeries(t, ts)
				ts = ts.Add(writeInterval)
			}
		})
	}
}

func TestMergeCopiesPerSeries(t *testing.T) {
	groups := [][]*prompb.TimeSeries{
		{
			{Labels: []*prompb.Label{{Name: "wave", Value: "1"}}, Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}}},
			{Labels: []*prompb.Label{{Name: "wave", Value: "2"}}, Samples: []prompb.Sample{{Timestamp: 1000, Value: 2}}},
		},
		{
			{Labels: []*prompb.Label{{Name: "wave", Value: "1"}}, Samples: []prompb.Sample{{Timestamp: 1000, Value: 10}}},
			{Labels: []*prompb.Label{{Name: "wave", Value: "2"}}, Samples: []prompb.Sample{{Timestamp: 1000, Value: 20}}},
		},
	}

	expected := []*prompb.TimeSeries{
		{Labels: []*prompb.Label{{Name: "wave", Value: "1"}}, Samples: []prompb.Sample{{Timestamp: 1000, Value: 1}, {Timestamp: 1000, Value: 10}}},
		{Labels: []*prompb.Label{{Name: "wave", Value: "2"}}, Samples: []prompb.Sample{{Timestamp: 1000, Value: 2}, {Timestamp: 1000, Value: 20}}},
	}

	assert.Equal(t, expected, mergeCopiesPerSeries(groups))
}

func TestGenerateSineWaveSeries_OutOfOrder(t *testing.T) {
	const (
		numSeries     = 3
		churnPeriod   = 0
		writeInterval = 10 * time.Second
	)

	testCases := map[string]struct {
		replicasPerSample int
		valueStrategy     DuplicatedSamplesValueStrategy
	}{
		"single occurrence": {
			replicasPerSample: 1,
			valueStrategy:     SameValue,
		},
		"multiple occurrences, same value": {
			replicasPerSample: 4,
			valueStrategy:     SameValue,
		},
		"multiple occurrences, different value": {
			replicasPerSample: 4,
			valueStrategy:     DifferentValue,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assertGeneratedSeries := func(t *testing.T, ts time.Time) {
				oooTs := ts.Add(-oooTimestampOffset)
				currentValue := generateSineWaveValue(ts)

				// expected[0] is the current-timestamp wave (never duplicated).
				// expected[1..replicasPerSample] are the out-of-order wave's
				// occurrences, at oooTs.
				expected := make([][]*prompb.TimeSeries, tc.replicasPerSample+1)
				expected[0] = make([]*prompb.TimeSeries, 0, numSeries)
				for seriesID := 1; seriesID <= numSeries; seriesID++ {
					expected[0] = append(expected[0], &prompb.TimeSeries{
						Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "wave", Value: strconv.Itoa(seriesID)}},
						Samples: []prompb.Sample{{Timestamp: ts.UnixMilli(), Value: currentValue}},
					})
				}

				for r := 0; r < tc.replicasPerSample; r++ {
					oooValue := generateSineWaveValue(oooTs)
					if tc.valueStrategy == DifferentValue && r > 0 {
						oooValue = generateSineWaveValue(oooTs.Add(time.Duration(r) * time.Millisecond))
					}

					expected[r+1] = make([]*prompb.TimeSeries, 0, numSeries)
					for seriesID := 1; seriesID <= numSeries; seriesID++ {
						expected[r+1] = append(expected[r+1], &prompb.TimeSeries{
							Labels:  []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "wave", Value: strconv.Itoa(seriesID)}},
							Samples: []prompb.Sample{{Timestamp: oooTs.UnixMilli(), Value: oooValue}},
						})
					}
				}

				cfg := WriteClientConfig{
					SeriesCount:                           numSeries,
					SeriesChurnPeriod:                     churnPeriod,
					ReplicasPerSample:                     tc.replicasPerSample,
					DuplicatedSamplesValueStrategy:        tc.valueStrategy,
					DuplicatedSamplesDistributionStrategy: OutOfOrder,
					WriteInterval:                         writeInterval,
				}
				assert.Equal(t, expected, generateSineWaveSeries(ts, cfg))
			}

			ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				assertGeneratedSeries(t, ts)
				ts = ts.Add(writeInterval)
			}
		})
	}
}

func labelValue(labels []*prompb.Label, name string) string {
	for _, l := range labels {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

func TestWriteClient_DifferentRequestBatchesEachCopyLevelSeparately(t *testing.T) {
	const (
		numSeries         = 5
		replicasPerSample = 3
		writeBatchSize    = 2
	)

	var (
		mu       sync.Mutex
		requests []*prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err := snappy.Decode(nil, compressed)
		require.NoError(t, err)

		var req prompb.WriteRequest
		require.NoError(t, proto.Unmarshal(data, &req))

		mu.Lock()
		requests = append(requests, &req)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := WriteClientConfig{
		URL:                                   *serverURL,
		UserID:                                "test",
		SeriesCount:                           numSeries,
		ReplicasPerSample:                     replicasPerSample,
		DuplicatedSamplesDistributionStrategy: DifferentRequest,
		WriteInterval:                         10 * time.Second,
		WriteTimeout:                          5 * time.Second,
		WriteConcurrency:                      10,
		WriteBatchSize:                        writeBatchSize,
	}

	c := NewWriteClient(cfg, log.NewNopLogger())
	c.writeSeries()

	// Each of the replicasPerSample copy levels is batched independently, in chunks of
	// at most writeBatchSize series: ceil(numSeries/writeBatchSize) requests per level.
	requestsPerLevel := (numSeries + writeBatchSize - 1) / writeBatchSize
	require.Len(t, requests, replicasPerSample*requestsPerLevel)

	seriesByWave := map[string][]*prompb.TimeSeries{}
	for _, req := range requests {
		assert.LessOrEqual(t, len(req.Timeseries), writeBatchSize)

		// A single request must never mix multiple copies of the same series: that
		// would mean it crossed copy-level boundaries instead of batching within one.
		seenWaves := map[string]bool{}
		for _, series := range req.Timeseries {
			wave := labelValue(series.Labels, "wave")
			require.Falsef(t, seenWaves[wave], "wave %s appears twice in the same request", wave)
			seenWaves[wave] = true

			seriesByWave[wave] = append(seriesByWave[wave], series)
		}
	}

	require.Len(t, seriesByWave, numSeries)
	for wave, series := range seriesByWave {
		assert.Lenf(t, series, replicasPerSample, "wave %s", wave)
		for _, s := range series {
			assert.Equal(t, series[0].Labels, s.Labels)
			assert.Equal(t, series[0].Samples, s.Samples)
		}
	}
}

func TestWriteClient_SameSeriesMergesCopiesIntoOneSeriesEntry(t *testing.T) {
	const (
		numSeries         = 2
		replicasPerSample = 3
	)

	var (
		mu       sync.Mutex
		requests []*prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err := snappy.Decode(nil, compressed)
		require.NoError(t, err)

		var req prompb.WriteRequest
		require.NoError(t, proto.Unmarshal(data, &req))

		mu.Lock()
		requests = append(requests, &req)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := WriteClientConfig{
		URL:                                   *serverURL,
		UserID:                                "test",
		SeriesCount:                           numSeries,
		ReplicasPerSample:                     replicasPerSample,
		DuplicatedSamplesDistributionStrategy: SameSeries,
		WriteInterval:                         10 * time.Second,
		WriteTimeout:                          5 * time.Second,
		WriteConcurrency:                      10,
		WriteBatchSize:                        1000,
	}

	c := NewWriteClient(cfg, log.NewNopLogger())
	c.writeSeries()

	// A single request, holding one merged series per wave (rather than one per copy).
	require.Len(t, requests, 1)
	require.Len(t, requests[0].Timeseries, numSeries)

	for _, series := range requests[0].Timeseries {
		assert.Lenf(t, series.Samples, replicasPerSample, "wave %s", labelValue(series.Labels, "wave"))
	}
}

func TestWriteClient_DifferentSeriesBatchesAllCopiesTogether(t *testing.T) {
	const (
		numSeries         = 2
		replicasPerSample = 3
	)

	var (
		mu       sync.Mutex
		requests []*prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err := snappy.Decode(nil, compressed)
		require.NoError(t, err)

		var req prompb.WriteRequest
		require.NoError(t, proto.Unmarshal(data, &req))

		mu.Lock()
		requests = append(requests, &req)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := WriteClientConfig{
		URL:                                   *serverURL,
		UserID:                                "test",
		SeriesCount:                           numSeries,
		ReplicasPerSample:                     replicasPerSample,
		DuplicatedSamplesDistributionStrategy: DifferentSeries,
		WriteInterval:                         10 * time.Second,
		WriteTimeout:                          5 * time.Second,
		WriteConcurrency:                      10,
		WriteBatchSize:                        1000,
	}

	c := NewWriteClient(cfg, log.NewNopLogger())
	c.writeSeries()

	// A single request, holding every copy as its own series entry (rather than one
	// merged entry per wave, or one request per copy).
	require.Len(t, requests, 1)
	require.Len(t, requests[0].Timeseries, numSeries*replicasPerSample)

	seriesByWave := map[string][]*prompb.TimeSeries{}
	for _, series := range requests[0].Timeseries {
		require.Len(t, series.Samples, 1)
		wave := labelValue(series.Labels, "wave")
		seriesByWave[wave] = append(seriesByWave[wave], series)
	}

	require.Len(t, seriesByWave, numSeries)
	for wave, series := range seriesByWave {
		assert.Lenf(t, series, replicasPerSample, "wave %s", wave)
	}
}

func TestWriteClient_DifferentSeriesKeepsEachSeriesDuplicatesInTheSameRequest(t *testing.T) {
	const (
		numSeries         = 6
		replicasPerSample = 4
		writeBatchSize    = 8 // a multiple of replicasPerSample, so batching splits between series, never within one.
	)

	var (
		mu       sync.Mutex
		requests []*prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err := snappy.Decode(nil, compressed)
		require.NoError(t, err)

		var req prompb.WriteRequest
		require.NoError(t, proto.Unmarshal(data, &req))

		mu.Lock()
		requests = append(requests, &req)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := WriteClientConfig{
		URL:                                   *serverURL,
		UserID:                                "test",
		SeriesCount:                           numSeries,
		ReplicasPerSample:                     replicasPerSample,
		DuplicatedSamplesValueStrategy:        DifferentValue,
		DuplicatedSamplesDistributionStrategy: DifferentSeries,
		WriteInterval:                         10 * time.Second,
		WriteTimeout:                          5 * time.Second,
		WriteConcurrency:                      10,
		WriteBatchSize:                        writeBatchSize,
	}

	c := NewWriteClient(cfg, log.NewNopLogger())
	c.writeSeries()

	requestsPerWave := map[string]int{}
	for _, req := range requests {
		assert.LessOrEqual(t, len(req.Timeseries), writeBatchSize)

		wavesInThisRequest := map[string]int{}
		for _, series := range req.Timeseries {
			wave := labelValue(series.Labels, "wave")
			wavesInThisRequest[wave]++
		}

		// A wave present in this request must show up with all its replicas: if it
		// were split across two requests, it would appear here with fewer than
		// replicasPerSample entries.
		for wave, count := range wavesInThisRequest {
			require.Equalf(t, replicasPerSample, count, "wave %s split across requests", wave)
			requestsPerWave[wave]++
		}
	}

	require.Len(t, requestsPerWave, numSeries)
	for wave, count := range requestsPerWave {
		assert.Equalf(t, 1, count, "wave %s appeared in more than one request", wave)
	}
}

func TestWriteClient_OutOfOrderSendsWavesSequentially(t *testing.T) {
	const (
		numSeries         = 2
		replicasPerSample = 3
		wave0Delay        = 100 * time.Millisecond
	)

	var (
		mu          sync.Mutex
		requests    []*prompb.WriteRequest
		arrivalTime []time.Time
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err := snappy.Decode(nil, compressed)
		require.NoError(t, err)

		var req prompb.WriteRequest
		require.NoError(t, proto.Unmarshal(data, &req))

		mu.Lock()
		isFirst := len(requests) == 0
		requests = append(requests, &req)
		arrivalTime = append(arrivalTime, time.Now())
		mu.Unlock()

		if isFirst {
			// Simulate a slow ack for the current-timestamp wave, to prove the
			// client waits for it before sending any out-of-order wave.
			time.Sleep(wave0Delay)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cfg := WriteClientConfig{
		URL:                                   *serverURL,
		UserID:                                "test",
		SeriesCount:                           numSeries,
		ReplicasPerSample:                     replicasPerSample,
		DuplicatedSamplesDistributionStrategy: OutOfOrder,
		WriteInterval:                         10 * time.Second,
		WriteTimeout:                          5 * time.Second,
		WriteConcurrency:                      10,
		WriteBatchSize:                        1000,
	}

	c := NewWriteClient(cfg, log.NewNopLogger())

	start := time.Now()
	c.writeSeries()

	// replicasPerSample+1 waves, one request each (batch size fits everything).
	require.Len(t, requests, replicasPerSample+1)

	// Every request after the first must have arrived only after wave 0's
	// (artificially slow) response, proving writeSeries() waited for it before
	// sending the next wave.
	for i := 1; i < len(arrivalTime); i++ {
		assert.GreaterOrEqualf(t, arrivalTime[i].Sub(start), wave0Delay, "request %d arrived before wave 0 was acknowledged", i)
	}

	// Content: request 0 carries the current timestamp, the rest carry the
	// out-of-order timestamp (oooTimestampOffset behind), each with numSeries series.
	require.Len(t, requests[0].Timeseries, numSeries)
	for _, series := range requests[0].Timeseries {
		require.Len(t, series.Samples, 1)
	}
	currentTs := requests[0].Timeseries[0].Samples[0].Timestamp
	oooTs := currentTs - oooTimestampOffset.Milliseconds()

	for _, req := range requests[1:] {
		require.Len(t, req.Timeseries, numSeries)
		for _, series := range req.Timeseries {
			require.Len(t, series.Samples, 1)
			assert.Equal(t, oooTs, series.Samples[0].Timestamp)
		}
	}
}
