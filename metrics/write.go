package metrics

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/open-fresh/avalanche/pkg/download"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	"github.com/open-fresh/avalanche/pkg/errors"
	dto "github.com/prometheus/client_model/go"
)

const maxErrMsgLen = 256

var (
	userAgent     = "avalanche"
	valRandomizer = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// ConfigWrite for the remote write requests.
type ConfigWrite struct {
	URL             url.URL
	RequestInterval time.Duration
	WritersBase,
	WritersCount,
	BatchSize,
	RequestCount int
	UpdateNotify []chan struct{}
	PprofURLs    []*url.URL
	Tenant       string
	AccessToken  string
}

// Client for the remote write requests.
type Client struct {
	clients []http.Client
	timeout time.Duration
	config  ConfigWrite
}

// SetupHTTPClients initializes HttpClients (one per writer)
func SetupHTTPClients(config ConfigWrite) ([]http.Client, error) {
	clients := []http.Client{}
	for w := 0; w < config.WritersCount; w++ {
		var httpClient *http.Client
		if config.AccessToken != "" {
			tlsConf := &tls.Config{InsecureSkipVerify: true}
			var rt http.RoundTripper = &http.Transport{TLSClientConfig: tlsConf}
			rt = &sysdigRoundTripper{apiToken: config.AccessToken, rt: rt}
			httpClient = &http.Client{Transport: rt}
		} else {
			var rt http.RoundTripper = &http.Transport{}
			rt = &cortexTenantRoundTripper{tenant: config.Tenant, rt: rt}
			httpClient = &http.Client{Transport: rt}
		}
		clients = append(clients, *httpClient)
	}
	return clients, nil
}

// SendRemoteWrite initializes a http client and
// sends metrics to a prometheus compatible remote endpoint.
func SendRemoteWrite(config ConfigWrite) error {
	clients, error := SetupHTTPClients(config)
	if error != nil {
		return error
	}

	c := Client{
		clients: clients,
		timeout: time.Minute,
		config:  config,
	}
	if config.WritersCount == 1 {
		return c.write(config.WritersBase, 0)
	}

	var wg sync.WaitGroup
	wg.Add(config.WritersCount)
	for w := 0; w < config.WritersCount; w++ {
		lw := w
		go func() {
			c.write(config.WritersBase, lw)
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

// Add the tenant ID header required by Cortex
func (rt *cortexTenantRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = cloneRequest(req)
	req.Header.Set("X-Scope-OrgID", rt.tenant)
	return rt.rt.RoundTrip(req)
}

type cortexTenantRoundTripper struct {
	tenant string
	rt     http.RoundTripper
}

// Add the tenant ID header required by Cortex
func (rt *sysdigRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = cloneRequest(req)
	req.Header.Set("Authorization", "bearer "+rt.apiToken)
	return rt.rt.RoundTrip(req)
}

type sysdigRoundTripper struct {
	apiToken string
	rt       http.RoundTripper
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request) *http.Request {
	// Shallow copy of the struct.
	r2 := new(http.Request)
	*r2 = *r
	// Deep copy of the Header.
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}
	return r2
}

func (c *Client) write(baseWriter int, writer int) error {

	tss, err := collectMetrics(baseWriter + writer)
	if err != nil {
		return err
	}

	var (
		totalTime       time.Duration
		totalSamplesExp = len(tss) * c.config.RequestCount
		totalSamplesAct int
		mtx             sync.Mutex
		wgMetrics       sync.WaitGroup
		wgPprof         sync.WaitGroup
		merr            = &errors.MultiError{}
	)

	log.Printf("Writer[%03d] sending:  %v timeseries, %v samples, %v timeseries per request, %v delay between requests\n", (baseWriter + writer), len(tss), c.config.RequestCount, c.config.BatchSize, c.config.RequestInterval)
	ticker := time.NewTicker(c.config.RequestInterval)
	defer ticker.Stop()
	for ii := 0; ii < c.config.RequestCount; ii++ {
		// Download the pprofs during half of the iteration to get avarege readings.
		// Do that only when it is not set to take profiles at a given interval.
		if len(c.config.PprofURLs) > 0 && ii == c.config.RequestCount/2 {
			wgPprof.Add(1)
			go func() {
				download.URLs(c.config.PprofURLs, time.Now().Format("2-Jan-2006-15:04:05"))
				wgPprof.Done()
			}()
		}
		<-ticker.C
		select {
		case <-c.config.UpdateNotify[writer]:
			if writer == 0 || writer == 1 {
				log.Println("updating remote write metrics")
			}
			tss, err = collectMetrics(baseWriter + writer)
			if err != nil {
				merr.Add(err)
			}
		default:
			tss = updateTimetamps(tss)
		}

		start := time.Now()
		for i := 0; i < len(tss); i += c.config.BatchSize {
			wgMetrics.Add(1)
			go func(i int) {
				defer wgMetrics.Done()
				end := i + c.config.BatchSize
				if end > len(tss) {
					end = len(tss)
				}
				req := &prompb.WriteRequest{
					Timeseries: tss[i:end],
				}
				err := c.Store(context.TODO(), writer, req)
				if err != nil {
					merr.Add(err)
					return
				}
				mtx.Lock()
				totalSamplesAct += len(tss[i:end])
				mtx.Unlock()

			}(i)
		}
		wgMetrics.Wait()
		totalTime += time.Since(start)
		if merr.Count() > 20 {
			log.Printf("Too many errors (%d), exiting from writer: %d\n", merr.Count(), writer)
			merr.Add(fmt.Errorf("too many errors"))
			log.Println(merr.Error())
			return merr.Err()
		}
	}
	wgPprof.Wait()
	if c.config.RequestCount*len(tss) != totalSamplesAct {
		merr.Add(fmt.Errorf("total samples mismatch, exp:%v , act:%v", totalSamplesExp, totalSamplesAct))
	}
	log.Printf("Total request time: %v ; Total samples: %v; Samples/sec: %v\n", totalTime.Round(time.Second), totalSamplesAct, int(float64(totalSamplesAct)/totalTime.Seconds()))
	return merr.Err()
}

func updateTimetamps(tss []prompb.TimeSeries) []prompb.TimeSeries {
	t := int64(model.Now())
	for i := range tss {
		tss[i].Samples[0].Timestamp = t
	}
	return tss
}

func collectMetrics(writer int) ([]prompb.TimeSeries, error) {
	metricsMux.Lock()
	defer metricsMux.Unlock()
	metricFamilies, err := promRegistry.Gather()
	if err != nil {
		return nil, err
	}
	return ToTimeSeriesSlice(writer, metricFamilies), nil
}

// ToTimeSeriesSlice converts a slice of metricFamilies containing samples into a slice of TimeSeries
func ToTimeSeriesSlice(writer int, metricFamilies []*dto.MetricFamily) []prompb.TimeSeries {
	tss := make([]prompb.TimeSeries, 0, len(metricFamilies)*10)
	timestamp := int64(model.Now()) // Not using metric.TimestampMs because it is (always?) nil. Is this right?

	for familyIndex, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.Metric {
			labels := prompbLabels(writer, *metricFamily.Name, metric.Label)
			ts := prompb.TimeSeries{
				Labels: labels,
			}
			// Add a random component
			switch *metricFamily.Type {
			case dto.MetricType_COUNTER:
				ts.Samples = []prompb.Sample{{
					Value:     *metric.Counter.Value,
					Timestamp: timestamp,
				}}
			case dto.MetricType_GAUGE:
				ts.Samples = []prompb.Sample{{
					Value:     randomizeValue(writer, familyIndex, 100, *metric.Gauge.Value),
					Timestamp: timestamp,
				}}
			}
			tss = append(tss, ts)
		}
	}

	return tss
}

func prompbLabels(writer int, name string, label []*dto.LabelPair) []prompb.Label {
	ret := make([]prompb.Label, 0, len(label)+1)
	ret = append(ret, prompb.Label{
		Name:  model.MetricNameLabel,
		Value: name,
	})
	ret = append(ret, prompb.Label{
		Name:  "avalanche_writer_index",
		Value: fmt.Sprintf("writer_index_%06d", writer),
	})
	ret = append(ret, prompb.Label{
		Name:  "avalanche_writer_address",
		Value: fmt.Sprintf("writer_address_%06d", writer),
	})
	ret = append(ret, prompb.Label{
		Name:  "avalanche_writer_geo",
		Value: fmt.Sprintf("writer_geo_%06d", writer),
	})
	for _, pair := range label {
		ret = append(ret, prompb.Label{
			Name:  *pair.Name,
			Value: *pair.Value,
		})
	}
	sort.Slice(ret, func(i int, j int) bool {
		return ret[i].Name < ret[j].Name
	})
	return ret
}

// Store sends a batch of samples to the HTTP endpoint.
func (c *Client) Store(ctx context.Context, writer int, req *prompb.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.config.URL.String(), bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	httpResp, err := c.clients[writer].Do(httpReq.WithContext(ctx))
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

func getRandomSign() int {
	if valRandomizer.Intn(2) == 0 {
		return 1
	}
	return -1
}

func randomizeValue(writer int, familyIndex int, max int, curValue float64) float64 {
	if writer == 0 || curValue <= 5 {
		return curValue
	}
	sign := getRandomSign()
	newValue := (float64)(sign*valRandomizer.Intn(30)) + curValue
	if newValue < 0.0 {
		return 0.0
	}
	if newValue > 99.0 {
		return 99.0
	}
	return newValue
}
