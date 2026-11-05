package scalers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	v2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type groundcoverMetadata struct {
	ServerAddress       string        `keda:"name=serverAddress,order=triggerMetadata;authParams"`
	BackendID           string        `keda:"name=backendId,order=triggerMetadata;authParams"`
	APIKey              string        `keda:"name=apiKey,order=authParams"`
	Query               string        `keda:"name=query,order=triggerMetadata"`
	ValueField          string        `keda:"name=valueField,order=triggerMetadata,optional"`
	Lookback            time.Duration `keda:"name=lookback,order=triggerMetadata,default=5m"`
	Delay               time.Duration `keda:"name=delay,order=triggerMetadata,default=1m"`
	Threshold           float64       `keda:"name=threshold,order=triggerMetadata"`
	ActivationThreshold float64       `keda:"name=activationThreshold,order=triggerMetadata,default=0"`
	triggerIndex        int
}

var groundcoverTimeReference = regexp.MustCompile(`(?i)\b_time\b`)

func (m *groundcoverMetadata) Validate() error {
	u, err := url.Parse(m.ServerAddress)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("serverAddress must be an HTTPS base URL without credentials, query parameters or a fragment")
	}
	if strings.TrimSpace(m.BackendID) == "" || strings.TrimSpace(m.APIKey) == "" || strings.TrimSpace(m.Query) == "" {
		return errors.New("backendId, query and an apiKey from TriggerAuthentication are required")
	}
	if groundcoverTimeReference.MatchString(m.Query) {
		return errors.New("query must not reference _time; use lookback and delay to select the scaling window")
	}
	if m.Lookback != time.Minute && m.Lookback != 5*time.Minute && m.Lookback != 10*time.Minute {
		return errors.New("lookback must be 1m, 5m or 10m")
	}
	if m.Delay < 30*time.Second || m.Delay > 5*time.Minute {
		return errors.New("delay must be between 30s and 5m")
	}
	if math.IsNaN(m.Threshold) || math.IsInf(m.Threshold, 0) || m.Threshold < 0.001 {
		return errors.New("threshold must be a finite number of at least 0.001; KEDA uses milli-unit precision")
	}
	if math.IsNaN(m.ActivationThreshold) || math.IsInf(m.ActivationThreshold, 0) || m.ActivationThreshold < 0 {
		return errors.New("activationThreshold must be a finite, non-negative number")
	}
	return nil
}

type groundcoverScaler struct {
	metadata   *groundcoverMetadata
	metricType v2.MetricTargetType
	httpClient *http.Client
	endpoint   string
	now        func() time.Time
}

// NewGroundcoverScaler creates a scaler for a single gcQL APM result.
func NewGroundcoverScaler(config *scalersconfig.ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, err
	}
	meta := &groundcoverMetadata{triggerIndex: config.TriggerIndex}
	if err := config.TypedConfig(meta); err != nil {
		return nil, fmt.Errorf("invalid groundcover configuration: %w", err)
	}
	endpoint, err := url.JoinPath(meta.ServerAddress, "api/apm-measurements/search")
	if err != nil {
		return nil, err
	}
	client := kedautil.CreateHTTPClient(config.GlobalHTTPTimeout, false)
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &groundcoverScaler{
		metadata: meta, metricType: metricType, httpClient: client, endpoint: endpoint, now: time.Now,
	}, nil
}

func (s *groundcoverScaler) Close(context.Context) error {
	s.httpClient.CloseIdleConnections()
	return nil
}

func (s *groundcoverScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	return []v2.MetricSpec{{
		Type: v2.ExternalMetricSourceType,
		External: &v2.ExternalMetricSource{
			Metric: v2.MetricIdentifier{Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, "groundcover")},
			Target: GetMetricTargetMili(s.metricType, s.metadata.Threshold),
		},
	}}
}

type groundcoverSearchRequest struct {
	Start   time.Time                    `json:"start"`
	End     time.Time                    `json:"end"`
	Query   string                       `json:"query"`
	Sources []groundcoverSearchCondition `json:"sources"`
}

type groundcoverSearchCondition struct {
	Key     string                    `json:"key"`
	Origin  string                    `json:"origin"`
	Filters []groundcoverSearchFilter `json:"filters"`
}

type groundcoverSearchFilter struct {
	Op    string    `json:"op"`
	Value time.Time `json:"value"`
}

func (s *groundcoverScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	end := s.now().UTC().Add(-s.metadata.Delay).Truncate(time.Minute)
	request := groundcoverSearchRequest{
		Start: end.Add(-s.metadata.Lookback), End: end, Query: s.metadata.Query,
		// The search API includes its end timestamp; the extra filter excludes it.
		Sources: []groundcoverSearchCondition{{
			Key: "_time", Origin: "root", Filters: []groundcoverSearchFilter{{Op: "lt", Value: end}},
		}},
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+s.metadata.APIKey)
	req.Header.Set("X-Backend-Id", s.metadata.BackendID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("groundcover query failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("groundcover API returned HTTP %d", resp.StatusCode)
	}
	const maxResponseBytes = 1 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(data) > maxResponseBytes {
		return nil, false, errors.New("groundcover response exceeds 1 MiB; query must return one row")
	}
	value, err := groundcoverResultValue(data, s.metadata.ValueField)
	if err != nil {
		return nil, false, err
	}
	metric := GenerateMetricInMili(metricName, value)
	metric.Timestamp = metav1.NewTime(end)
	seconds := int64(s.metadata.Lookback / time.Second)
	metric.WindowSeconds = &seconds
	return []external_metrics.ExternalMetricValue{metric}, value > s.metadata.ActivationThreshold, nil
}

func groundcoverResultValue(data []byte, field string) (float64, error) {
	var response struct {
		Data            []map[string]json.RawMessage `json:"data"`
		ValueFieldHints []string                     `json:"valueFieldHints"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return 0, fmt.Errorf("invalid groundcover response: %w", err)
	}
	if len(response.Data) == 0 {
		return 0, errors.New("groundcover query returned no data for the scaling window")
	}
	if len(response.Data) != 1 {
		return 0, fmt.Errorf("groundcover query returned %d rows; narrow the filters to return one row", len(response.Data))
	}
	if field == "" {
		if len(response.ValueFieldHints) != 1 {
			return 0, errors.New("groundcover query must return one numeric field, or set valueField")
		}
		field = response.ValueFieldHints[0]
	}
	raw, ok := response.Data[0][field]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, fmt.Errorf("groundcover field %q is missing or null", field)
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, fmt.Errorf("groundcover field %q must contain a finite, non-negative JSON number", field)
	}
	return value, nil
}
