package client

import (
	"strconv"
	"testing"
	"time"

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

		cfg := WriteClientConfig{SeriesCount: numSeries, SeriesChurnPeriod: churnPeriod, SamplesPerSeries: 1, ReplicasPerSample: 1, WriteInterval: 10 * time.Second}
		assert.Equal(t, expected, generateSineWaveSeries(ts, cfg))
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

		cfg := WriteClientConfig{SeriesCount: numSeries, SeriesChurnPeriod: churnPeriod, SamplesPerSeries: 1, ReplicasPerSample: 1, WriteInterval: 10 * time.Second}
		assert.Equal(t, expected, generateSineWaveSeries(ts, cfg))
	}

	ts, err := time.Parse(time.RFC3339, "2023-06-29T00:00:00Z")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		assertGeneratedSeries(t, ts)
		ts = ts.Add(10 * time.Second)
	}
}

func TestGenerateSineWaveSeries_WithoutChurningSeries_WithDuplicates(t *testing.T) {
	const (
		numSeries     = 3
		churnPeriod   = 0
		writeInterval = 10 * time.Second
	)

	testCases := map[string]struct {
		samplesPerSeries     int
		replicasPerSample    int
		valueStrategy        DuplicatedSamplesValueStrategy
		distributionStrategy DuplicatedSamplesDistributionStrategy
	}{
		"single sample, no replicas": {
			samplesPerSeries:     1,
			replicasPerSample:    1,
			valueStrategy:        SameValue,
			distributionStrategy: SameSeries,
		},
		"multiple samples, no replicas": {
			samplesPerSeries:     10,
			replicasPerSample:    1,
			valueStrategy:        SameValue,
			distributionStrategy: SameSeries,
		},
		"single sample, multiple replicas, same value, same series": {
			samplesPerSeries:     1,
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: SameSeries,
		},
		"multiple samples, multiple replicas, same value, same series": {
			samplesPerSeries:     10,
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: SameSeries,
		},
		"single sample, multiple replicas, different value, same series": {
			samplesPerSeries:     1,
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: SameSeries,
		},
		"multiple samples, multiple replicas, different value, same series": {
			samplesPerSeries:     10,
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: SameSeries,
		},
		"single sample, multiple replicas, same value, different series": {
			samplesPerSeries:     1,
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: DifferentSeries,
		},
		"multiple samples, multiple replicas, same value, different series": {
			samplesPerSeries:     10,
			replicasPerSample:    4,
			valueStrategy:        SameValue,
			distributionStrategy: DifferentSeries,
		},
		"single sample, multiple replicas, different value, different series": {
			samplesPerSeries:     1,
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: DifferentSeries,
		},
		"multiple samples, multiple replicas, different value, different series": {
			samplesPerSeries:     10,
			replicasPerSample:    4,
			valueStrategy:        DifferentValue,
			distributionStrategy: DifferentSeries,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assertGeneratedSeries := func(t *testing.T, ts time.Time) {
				// samples[r][i] holds the i-th sample of replica r.
				samples := make([][]prompb.Sample, tc.replicasPerSample)
				for r := 0; r < tc.replicasPerSample; r++ {
					samples[r] = make([]prompb.Sample, 0, tc.samplesPerSeries)
				}

				for i := 0; i < tc.samplesPerSeries; i++ {
					sampleTs := ts.Add(writeInterval * time.Duration(i) / time.Duration(tc.samplesPerSeries))
					value := generateSineWaveValue(sampleTs)

					for r := 0; r < tc.replicasPerSample; r++ {
						replicaValue := value
						if tc.valueStrategy == DifferentValue && r > 0 {
							replicaValue = generateSineWaveValue(sampleTs.Add(time.Duration(r) * time.Millisecond))
						}

						samples[r] = append(samples[r], prompb.Sample{Timestamp: sampleTs.UnixMilli(), Value: replicaValue})
					}
				}

				expected := make([]*prompb.TimeSeries, 0, numSeries)
				for seriesID := 1; seriesID <= numSeries; seriesID++ {
					labels := []*prompb.Label{{Name: "__name__", Value: "cortex_load_generator_sine_wave"}, {Name: "wave", Value: strconv.Itoa(seriesID)}}

					if tc.distributionStrategy == DifferentSeries {
						for r := 0; r < tc.replicasPerSample; r++ {
							expected = append(expected, &prompb.TimeSeries{Labels: labels, Samples: samples[r]})
						}
						continue
					}

					seriesSamples := make([]prompb.Sample, 0, tc.samplesPerSeries*tc.replicasPerSample)
					for r := 0; r < tc.replicasPerSample; r++ {
						seriesSamples = append(seriesSamples, samples[r]...)
					}

					expected = append(expected, &prompb.TimeSeries{Labels: labels, Samples: seriesSamples})
				}

				cfg := WriteClientConfig{
					SeriesCount:                           numSeries,
					SeriesChurnPeriod:                     churnPeriod,
					SamplesPerSeries:                      tc.samplesPerSeries,
					ReplicasPerSample:                     tc.replicasPerSample,
					DuplicatedSamplesValueStrategy:        tc.valueStrategy,
					DuplicatedSamplesDistributionStrategy: tc.distributionStrategy,
					WriteInterval:                         writeInterval,
				}
				actual := generateSineWaveSeries(ts, cfg)
				assert.Equal(t, expected, actual)
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
