package metrics

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	promRegistry = prometheus.NewRegistry() // local Registry so we don't get Go metrics, etc.
	valGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	metrics      = make([]*prometheus.GaugeVec, 0)
	metricsMux   = &sync.Mutex{}
)

func registerMetrics(metricCount int, metricLength int, metricCycle int, labelKeys []string, inputMetricName string) {
	metrics = make([]*prometheus.GaugeVec, metricCount)
	for idx := 0; idx < metricCount; idx++ {
		var metricName string
		if idx == metricCount-1 && inputMetricName != "" {
			metricName = inputMetricName
		} else {
			metricName = fmt.Sprintf("avalanche_metric_%s_%v_%v", strings.Repeat("m", metricLength), metricCycle, idx)
		}
		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricName,
			Help: "A tasty metric morsel",
		}, append([]string{"series_id", "cycle_id"}, labelKeys...))
		promRegistry.MustRegister(gauge)
		metrics[idx] = gauge
	}
}

func unregisterMetrics() {
	for _, metric := range metrics {
		promRegistry.Unregister(metric)
	}
}

func seriesLabels(seriesID int, cycleID int, labelKeys []string, labelValues []string) prometheus.Labels {
	labels := prometheus.Labels{
		"series_id": fmt.Sprintf("%v", seriesID),
		"cycle_id":  fmt.Sprintf("%v", cycleID),
	}

	for idx, key := range labelKeys {
		labels[key] = labelValues[idx]
	}

	return labels
}

func deleteValues(labelKeys []string, labelValues []string, seriesCount int, seriesCycle int) {
	for _, metric := range metrics {
		for idx := 0; idx < seriesCount; idx++ {
			labels := seriesLabels(idx, seriesCycle, labelKeys, labelValues)
			metric.Delete(labels)
		}
	}
}

func cycleValues(labelKeys []string, labelValues []string, seriesCount int, seriesCycle int, valueCycle int) {
	for _, metric := range metrics {
		for idx := 0; idx < seriesCount; idx++ {
			labels := seriesLabels(idx, seriesCycle, labelKeys, labelValues)
			cycle := valueCycle % 20
			if cycle >= 11 && cycle <= 13 {
				metric.With(labels).Set(float64(valGenerator.Intn(5)))
			} else {
				metric.With(labels).Set(float64(60 + valGenerator.Intn(20)))
			}
		}
	}
}

// RunMetrics creates a set of Prometheus test series that update over time
func RunMetrics(writersCount int, metricCount int, labelCount int, seriesCount int, metricLength int, labelLength int, valueInterval int, seriesInterval int, metricInterval int, inputMetricName string, stop chan struct{}) ([]chan struct{}, error) {
	labelKeys := make([]string, labelCount, labelCount)
	for idx := 0; idx < labelCount; idx++ {
		labelKeys[idx] = fmt.Sprintf("avalanche_label_key_%s_%v", strings.Repeat("k", labelLength), idx)
	}
	labelValues := make([]string, labelCount, labelCount)
	for idx := 0; idx < labelCount; idx++ {
		labelValues[idx] = fmt.Sprintf("avalanche_label_val_%s_%v", strings.Repeat("v", labelLength), idx)
	}

	valueCycle := 0
	metricCycle := 0
	seriesCycle := 0
	registerMetrics(metricCount, metricLength, metricCycle, labelKeys, inputMetricName)
	cycleValues(labelKeys, labelValues, seriesCount, seriesCycle, valueCycle)
	valueTick := time.NewTicker(time.Duration(valueInterval) * time.Second)
	seriesTick := time.NewTicker(time.Duration(seriesInterval) * time.Second)
	metricTick := time.NewTicker(time.Duration(metricInterval) * time.Second)
	// One update channel per writer
	updateNotify := make([]chan struct{}, writersCount)
	for w := 0; w < writersCount; w++ {
		updateNotify[w] = make(chan struct{}, 1)
	}

	go func() {
		for tick := range valueTick.C {
			fmt.Printf("%v: refreshing metric values\n", tick)
			metricsMux.Lock()
			cycleValues(labelKeys, labelValues, seriesCount, seriesCycle, valueCycle)
			valueCycle++
			metricsMux.Unlock()
			sendToChannels(updateNotify)
		}
	}()

	go func() {
		for tick := range seriesTick.C {
			fmt.Printf("%v: refreshing series cycle\n", tick)
			metricsMux.Lock()
			deleteValues(labelKeys, labelValues, seriesCount, seriesCycle)
			seriesCycle++
			valueCycle = 0
			cycleValues(labelKeys, labelValues, seriesCount, seriesCycle, valueCycle)
			metricsMux.Unlock()
			sendToChannels(updateNotify)
		}
	}()

	go func() {
		for tick := range metricTick.C {
			fmt.Printf("%v: refreshing metric cycle\n", tick)
			metricsMux.Lock()
			metricCycle++
			unregisterMetrics()
			registerMetrics(metricCount, metricLength, metricCycle, labelKeys, inputMetricName)
			valueCycle = 0
			cycleValues(labelKeys, labelValues, seriesCount, seriesCycle, valueCycle)
			metricsMux.Unlock()
			sendToChannels(updateNotify)
		}
	}()

	go func() {
		<-stop
		valueTick.Stop()
		seriesTick.Stop()
		metricTick.Stop()
	}()

	return updateNotify, nil
}

func sendToChannels(channels []chan struct{}) {
	for _, c := range channels {
		c <- struct{}{}
	}
}

// ServeMetrics serves a prometheus metrics endpoint with test series
func ServeMetrics(port int) error {
	http.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
	err := http.ListenAndServe(fmt.Sprintf(":%v", port), nil)
	if err != nil {
		return err
	}

	return nil
}
