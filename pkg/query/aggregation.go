package query

import (
	"fmt"
	"math"
	"sort"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// AggregateFunc represents an aggregation function type.
type AggregateFunc string

const (
	// Sum aggregates by summing values
	Sum AggregateFunc = "sum"

	// Avg aggregates by averaging values
	Avg AggregateFunc = "avg"

	// Max aggregates by taking maximum value
	Max AggregateFunc = "max"

	// Min aggregates by taking minimum value
	Min AggregateFunc = "min"

	// Count aggregates by counting samples
	Count AggregateFunc = "count"

	// StdDev aggregates by calculating standard deviation
	StdDev AggregateFunc = "stddev"

	// StdVar aggregates by calculating variance
	StdVar AggregateFunc = "stdvar"
)

// AggregationQuery represents an aggregation query.
type AggregationQuery struct {
	// Base query
	Query *Query

	// Aggregation function
	Function AggregateFunc

	// Step interval for bucketing (e.g., 5 minutes)
	Step int64

	// Group by labels (for multi-series aggregation)
	GroupBy []string

	// Without labels (exclude these labels from grouping)
	Without []string
}

// AggregationResult represents the result of an aggregation.
type AggregationResult struct {
	// Grouped series results
	Series []AggregatedTimeSeries
}

// AggregatedTimeSeries represents a single aggregated time series.
type AggregatedTimeSeries struct {
	Labels  map[string]string
	Samples []series.Sample
}

// Aggregate executes an aggregation query.
func (qe *QueryEngine) Aggregate(aq *AggregationQuery) (*AggregationResult, error) {
	if aq == nil || aq.Query == nil {
		return nil, fmt.Errorf("aggregation query cannot be nil")
	}

	if aq.Step <= 0 {
		return nil, fmt.Errorf("step must be positive")
	}

	// Execute the base query
	result, err := qe.ExecQuery(aq.Query)
	if err != nil {
		return nil, err
	}

	// Group series by labels
	groups := qe.groupSeries(result.Series, aq.GroupBy, aq.Without)

	// Aggregate each group
	aggregated := &AggregationResult{
		Series: make([]AggregatedTimeSeries, 0, len(groups)),
	}

	for _, group := range groups {
		// Aggregate the series in this group
		samples, err := qe.aggregateGroup(group.Series, aq.Function, aq.Step, aq.Query.MinTime, aq.Query.MaxTime)
		if err != nil {
			return nil, fmt.Errorf("failed to aggregate group: %w", err)
		}

		aggregated.Series = append(aggregated.Series, AggregatedTimeSeries{
			Labels:  group.Labels,
			Samples: samples,
		})
	}

	return aggregated, nil
}

// groupSeries groups time series by labels.
func (qe *QueryEngine) groupSeries(seriesList []TimeSeries, groupBy []string, without []string) []struct {
	Labels  map[string]string
	Series  []TimeSeries
} {
	groups := make(map[string][]TimeSeries)
	groupLabels := make(map[string]map[string]string)

	for _, ts := range seriesList {
		// Compute group key
		groupKey, labels := computeGroupKey(ts.Labels, groupBy, without)

		groups[groupKey] = append(groups[groupKey], ts)
		groupLabels[groupKey] = labels
	}

	// Convert to slice of groups
	result := make([]struct {
		Labels  map[string]string
		Series  []TimeSeries
	}, 0, len(groups))

	for key, seriesGroup := range groups {
		result = append(result, struct {
			Labels  map[string]string
			Series  []TimeSeries
		}{
			Labels:  groupLabels[key],
			Series:  seriesGroup,
		})
	}

	return result
}

// computeGroupKey computes a grouping key and labels for a series.
func computeGroupKey(labels map[string]string, groupBy []string, without []string) (string, map[string]string) {
	groupLabels := make(map[string]string)

	if len(groupBy) > 0 {
		// Include only specified labels
		for _, label := range groupBy {
			if value, ok := labels[label]; ok {
				groupLabels[label] = value
			}
		}
	} else if len(without) > 0 {
		// Include all labels except specified ones
		for label, value := range labels {
			excluded := false
			for _, w := range without {
				if label == w {
					excluded = true
					break
				}
			}
			if !excluded {
				groupLabels[label] = value
			}
		}
	} else {
		// No grouping - all series together
		groupLabels = make(map[string]string)
	}

	// Create a stable string key from labels
	keys := make([]string, 0, len(groupLabels))
	for k := range groupLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	key := ""
	for _, k := range keys {
		key += k + "=" + groupLabels[k] + ","
	}

	return key, groupLabels
}

// aggregateGroup aggregates a group of time series.
func (qe *QueryEngine) aggregateGroup(seriesList []TimeSeries, fn AggregateFunc, step int64, minTime, maxTime int64) ([]series.Sample, error) {
	if len(seriesList) == 0 {
		return nil, nil
	}

	// Align samples to step boundaries
	buckets := make(map[int64][]float64)

	for _, ts := range seriesList {
		for _, sample := range ts.Samples {
			if sample.Timestamp < minTime || sample.Timestamp > maxTime {
				continue
			}

			// Align to step boundary
			bucketTime := (sample.Timestamp / step) * step
			buckets[bucketTime] = append(buckets[bucketTime], sample.Value)
		}
	}

	// Aggregate each bucket
	result := make([]series.Sample, 0, len(buckets))

	for bucketTime, values := range buckets {
		aggregatedValue, err := applyAggregation(values, fn)
		if err != nil {
			return nil, err
		}

		result = append(result, series.Sample{
			Timestamp: bucketTime,
			Value:     aggregatedValue,
		})
	}

	// Sort by timestamp
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result, nil
}

// applyAggregation applies an aggregation function to a set of values.
func applyAggregation(values []float64, fn AggregateFunc) (float64, error) {
	if len(values) == 0 {
		return 0, nil
	}

	switch fn {
	case Sum:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum, nil

	case Avg:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values)), nil

	case Max:
		max := math.Inf(-1)
		for _, v := range values {
			if v > max {
				max = v
			}
		}
		return max, nil

	case Min:
		min := math.Inf(1)
		for _, v := range values {
			if v < min {
				min = v
			}
		}
		return min, nil

	case Count:
		return float64(len(values)), nil

	case StdDev:
		// Calculate standard deviation
		if len(values) < 2 {
			return 0, nil
		}
		mean := 0.0
		for _, v := range values {
			mean += v
		}
		mean /= float64(len(values))

		variance := 0.0
		for _, v := range values {
			diff := v - mean
			variance += diff * diff
		}
		variance /= float64(len(values))

		return math.Sqrt(variance), nil

	case StdVar:
		// Calculate variance
		if len(values) < 2 {
			return 0, nil
		}
		mean := 0.0
		for _, v := range values {
			mean += v
		}
		mean /= float64(len(values))

		variance := 0.0
		for _, v := range values {
			diff := v - mean
			variance += diff * diff
		}
		variance /= float64(len(values))

		return variance, nil

	default:
		return 0, fmt.Errorf("unsupported aggregation function: %s", fn)
	}
}

// Rate calculates the per-second rate of increase over a time range.
// This is commonly used for counters that only increase.
//
// rate(v[5m]) calculates the per-second rate of increase averaged over 5 minutes.
func (qe *QueryEngine) Rate(q *Query, rangeSeconds int64) (*QueryResult, error) {
	if rangeSeconds <= 0 {
		return nil, fmt.Errorf("range must be positive")
	}

	// Execute base query
	result, err := qe.ExecQuery(q)
	if err != nil {
		return nil, err
	}

	// Calculate rate for each series
	rateResult := &QueryResult{
		Series: make([]TimeSeries, 0, len(result.Series)),
	}

	for _, ts := range result.Series {
		if len(ts.Samples) < 2 {
			continue // Need at least 2 samples
		}

		rateSamples := make([]series.Sample, 0, len(ts.Samples)-1)

		for i := 1; i < len(ts.Samples); i++ {
			prev := ts.Samples[i-1]
			curr := ts.Samples[i]

			timeDiff := float64(curr.Timestamp-prev.Timestamp) / 1000.0 // Convert ms to seconds
			if timeDiff <= 0 {
				continue
			}

			valueDiff := curr.Value - prev.Value

			// Handle counter resets (value decreases)
			if valueDiff < 0 {
				valueDiff = curr.Value // Assume reset to 0
			}

			rate := valueDiff / timeDiff

			rateSamples = append(rateSamples, series.Sample{
				Timestamp: curr.Timestamp,
				Value:     rate,
			})
		}

		if len(rateSamples) > 0 {
			rateResult.Series = append(rateResult.Series, TimeSeries{
				Labels:  ts.Labels,
				Samples: rateSamples,
			})
		}
	}

	return rateResult, nil
}

// Increase calculates the total increase over a time range.
// This is commonly used for counters.
//
// increase(v[5m]) calculates the total increase over 5 minutes.
func (qe *QueryEngine) Increase(q *Query) (*QueryResult, error) {
	// Execute base query
	result, err := qe.ExecQuery(q)
	if err != nil {
		return nil, err
	}

	// Calculate increase for each series
	increaseResult := &QueryResult{
		Series: make([]TimeSeries, 0, len(result.Series)),
	}

	for _, ts := range result.Series {
		if len(ts.Samples) < 2 {
			continue // Need at least 2 samples
		}

		first := ts.Samples[0]
		last := ts.Samples[len(ts.Samples)-1]

		increase := last.Value - first.Value

		// Handle counter resets
		if increase < 0 {
			increase = last.Value // Assume reset
		}

		increaseSamples := []series.Sample{
			{
				Timestamp: last.Timestamp,
				Value:     increase,
			},
		}

		increaseResult.Series = append(increaseResult.Series, TimeSeries{
			Labels:  ts.Labels,
			Samples: increaseSamples,
		})
	}

	return increaseResult, nil
}

// Delta calculates the difference between the first and last value.
// Unlike increase, it can be negative.
func (qe *QueryEngine) Delta(q *Query) (*QueryResult, error) {
	// Execute base query
	result, err := qe.ExecQuery(q)
	if err != nil {
		return nil, err
	}

	// Calculate delta for each series
	deltaResult := &QueryResult{
		Series: make([]TimeSeries, 0, len(result.Series)),
	}

	for _, ts := range result.Series {
		if len(ts.Samples) < 2 {
			continue
		}

		first := ts.Samples[0]
		last := ts.Samples[len(ts.Samples)-1]

		delta := last.Value - first.Value

		deltaSamples := []series.Sample{
			{
				Timestamp: last.Timestamp,
				Value:     delta,
			},
		}

		deltaResult.Series = append(deltaResult.Series, TimeSeries{
			Labels:  ts.Labels,
			Samples: deltaSamples,
		})
	}

	return deltaResult, nil
}

// Derivative calculates the per-second derivative (rate of change).
// Similar to rate() but doesn't handle counter resets.
func (qe *QueryEngine) Derivative(q *Query) (*QueryResult, error) {
	// Execute base query
	result, err := qe.ExecQuery(q)
	if err != nil {
		return nil, err
	}

	// Calculate derivative for each series
	derivResult := &QueryResult{
		Series: make([]TimeSeries, 0, len(result.Series)),
	}

	for _, ts := range result.Series {
		if len(ts.Samples) < 2 {
			continue
		}

		derivSamples := make([]series.Sample, 0, len(ts.Samples)-1)

		for i := 1; i < len(ts.Samples); i++ {
			prev := ts.Samples[i-1]
			curr := ts.Samples[i]

			timeDiff := float64(curr.Timestamp-prev.Timestamp) / 1000.0 // Convert ms to seconds
			if timeDiff <= 0 {
				continue
			}

			valueDiff := curr.Value - prev.Value
			derivative := valueDiff / timeDiff

			derivSamples = append(derivSamples, series.Sample{
				Timestamp: curr.Timestamp,
				Value:     derivative,
			})
		}

		if len(derivSamples) > 0 {
			derivResult.Series = append(derivResult.Series, TimeSeries{
				Labels:  ts.Labels,
				Samples: derivSamples,
			})
		}
	}

	return derivResult, nil
}
