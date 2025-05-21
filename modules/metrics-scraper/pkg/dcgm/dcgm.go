// Copyright 2024 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dcgm

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/klog/v2"

	"github.com/prometheus/common/expfmt"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Collector handles DCGM metrics collection
type Collector struct {
	client    *kubernetes.Clientset
	namespace string
	service   string
	port      string
	suffix    string
}

// DCGMMetric represents a single DCGM metric entry
type GPUMetric struct {
	UID               string
	Name              string
	Namespace         string
	Container         string
	GPUId             string
	GPUUtilization    int64
	MemoryUtilization int64
	MemoryUsed        int64
	MemoryTotal       int64
	PowerUsage        int64
	Temperature       int64
}

// DCGM_FI_DRIVER_VERSION="550.144.03",
// container="main"
// namespace="sandboxaq"
// pod="ic50-priority-2-3950-stf4x"} 13055
type DCGMMetric struct {
	Name      string  `json:"name"`
	GPU       string  `json:"gpu"`
	UUID      string  `json:"uuid"`
	PCIBusID  string  `json:"pci_bus_id"`
	Device    string  `json:"device"`
	ModelName string  `json:"model_name"`
	Hostname  string  `json:"hostname"`
	DriverVer string  `json:"driver_version"`
	Container string  `json:"container"`
	Namespace string  `json:"namespace"`
	Pod       string  `json:"pod"`
	Value     float64 `json:"value"`
}

type DCGMMetricList struct {
	Metrics []DCGMMetric
}

// NewCollector creates a new DCGM metrics collector
func NewCollector(config *rest.Config, serviceEndpoint string) *Collector {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Unable to generate a clientset: %s", err)
	}
	namespace, service, port, suffix, err := splitServiceEndpoint(serviceEndpoint)
	if err != nil {
		klog.Fatalf("Unable to split service endpoint: %s", err)
	}
	return &Collector{
		client:    clientset,
		namespace: namespace,
		service:   service,
		port:      port,
		suffix:    suffix,
	}
}

// CollectMetrics collects DCGM metrics for all GPUs
func (c *Collector) CollectMetrics() (*DCGMMetricList, error) {
	// Call the kubernetes API to get the metrics
	res := c.client.CoreV1().RESTClient().Get().
		Namespace(c.namespace).
		Resource("pods").
		Name(c.service + ":" + c.port).
		SubResource("proxy").
		Suffix(c.suffix).
		Do(context.Background())
	// Get the raw response
	resp, err := res.Raw()
	klog.Infof("Response: %#v", resp)

	if err != nil {
		return nil, fmt.Errorf("failed to get metrics from DCGM exporter: %w", err)
	}

	metrics, err := parseMetrics(string(resp))
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics: %w", err)
	}

	return metrics, nil
}

func splitServiceEndpoint(input string) (namespace, service, port, suffix string, err error) {
	// Compile a regex to match: namespace/service:port/optional/path
	re := regexp.MustCompile(`^([^/]+)/([^:]+):([^/]+)(/.*)?$`)
	matches := re.FindStringSubmatch(input)
	if matches == nil {
		return "", "", "", "", fmt.Errorf("input does not match expected format")
	}
	namespace = matches[1]
	service = matches[2]
	port = matches[3]
	suffix = matches[4]
	if len(suffix) > 0 {
		suffix = strings.TrimPrefix(suffix, "/")
	}
	return namespace, service, port, suffix, nil
}

// parseMetrics parses the Prometheusinput string and returns a list of DCGMMetric
func parseMetrics(input string) (*DCGMMetricList, error) {
	// Ensure input ends with a newline for Prometheus parser
	if !strings.HasSuffix(input, "\n") {
		input += "\n"
	}

	parser := expfmt.TextParser{}
	mfs, err := parser.TextToMetricFamilies(strings.NewReader(input))

	if err != nil {
		return nil, err
	}

	var metrics DCGMMetricList
	for _, mf := range mfs {
		metric := DCGMMetric{}
		metric.Name = mf.GetName()
		for _, m := range mf.Metric {
			for _, l := range m.Label {
				switch l.GetName() {
				case "gpu":
					metric.GPU = l.GetValue()
				case "UUID":
					metric.UUID = l.GetValue()
				case "pci_bus_id":
					metric.PCIBusID = l.GetValue()
				case "device":
					metric.Device = l.GetValue()
				case "modelName":
					metric.ModelName = l.GetValue()
				case "Hostname":
					metric.Hostname = l.GetValue()
				case "DCGM_FI_DRIVER_VERSION":
					metric.DriverVer = l.GetValue()
				case "container":
					metric.Container = l.GetValue()
				case "namespace":
					metric.Namespace = l.GetValue()
				case "pod":
					metric.Pod = l.GetValue()
				}
			}
			if m.Untyped != nil {
				metric.Value = m.Untyped.GetValue()
			}
			metrics.Metrics = append(metrics.Metrics, metric)
		}
	}
	return &metrics, nil
}
