package nodeutilization

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"k8s.io/klog/v2"
)

const (
	BaseUrl         = "/watcher"
	httpClientTimeoutSeconds = 10*time.Second
)

type Window struct {
	Duration string `json:"duration"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
}

type Metric struct {
	Name     string  `json:"name"`             // Name of metric at the provider
	Type     string  `json:"type"`             // CPU or Memory
	Operator string  `json:"operator"`         // STD or AVG or SUM, etc.
	Rollup   string  `json:"rollup,omitempty"` // Rollup used for metric calculation
	Value    float64 `json:"value"`            // Value is expected to be in %
}

type NodeMetricsMap map[string]NodeMetrics

type Data struct {
	NodeMetricsMap NodeMetricsMap
}

type WatcherMetrics struct {
	Timestamp int64  `json:"timestamp"`
	Window    Window `json:"window"`
	Source    string `json:"source"`
	Data      Data   `json:"data"`
}

type Tags struct {
}

type Metadata struct {
	DataCenter string `json:"dataCenter,omitempty"`
}

type NodeMetrics struct {
	Metrics  []Metric `json:"metrics,omitempty"`
	Tags     Tags     `json:"tags,omitempty"`
	Metadata Metadata `json:"metadata,omitempty"`
}

type serviceClient struct {
	httpClient     http.Client
	watcherAddress string
}

func (c serviceClient) GetWatcherMetrics() (*WatcherMetrics, error) {
	req, err := http.NewRequest(http.MethodGet, c.watcherAddress+BaseUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	//TODO(aqadeer): Add a couple of retries for transient errors
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	klog.V(6).Infof("received status code %v from watcher", resp.StatusCode)
	if resp.StatusCode == http.StatusOK {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		bodyS := buf.String()
		data := Data{NodeMetricsMap: make(map[string]NodeMetrics)}
		metrics := WatcherMetrics{Data: data}
		//dec := gojay.BorrowDecoder(resp.Body)
		//defer dec.Release()
		//err = dec.Decode(&metrics)
		err := json.Unmarshal([]byte(bodyS), &metrics)
		if err != nil {
			klog.Errorf("unable to decode watcher metrics: %v", err)
			return nil, err
		} else {
			return &metrics, nil
		}
	} else {
		klog.Errorf("received status code %v from watcher", resp.StatusCode)
	}
	return nil, nil
}

func NewServiceClient(watcherAddress string) (*serviceClient, error) {
	return &serviceClient{
		httpClient: http.Client{
			Timeout: httpClientTimeoutSeconds,
		},
		watcherAddress: watcherAddress,
	}, nil
}