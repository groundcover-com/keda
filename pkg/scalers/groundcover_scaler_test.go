package scalers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	v2 "k8s.io/api/autoscaling/v2"
	"sigs.k8s.io/yaml"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

const groundcoverTestQuery = "resource_type:http is_inbound:true source:eBPF workload:api | stats by(workload) sum(total_counter) as requests | math requests / 300 as rps | fields rps"

func groundcoverTestConfig(address string) *scalersconfig.ScalerConfig {
	return &scalersconfig.ScalerConfig{
		TriggerType:       "groundcover",
		TriggerIndex:      2,
		GlobalHTTPTimeout: time.Second,
		TriggerMetadata: map[string]string{
			"serverAddress": address,
			"backendId":     "example-backend",
			"query":         groundcoverTestQuery,
			"threshold":     "60",
		},
		AuthParams: map[string]string{"apiKey": "test-api-key"},
	}
}

func groundcoverTestScaler(t *testing.T, server *httptest.Server, config *scalersconfig.ScalerConfig) *groundcoverScaler {
	t.Helper()
	scaler, err := NewGroundcoverScaler(config)
	require.NoError(t, err)
	s, ok := scaler.(*groundcoverScaler)
	require.True(t, ok)
	s.httpClient.Transport = server.Client().Transport
	s.now = func() time.Time { return time.Date(2026, 11, 5, 12, 3, 17, 0, time.UTC) }
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	return s
}

func TestGroundcoverPostAndRollingWindow(t *testing.T) {
	requests := make(chan map[string]any, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/apm-measurements/search", r.URL.Path)
		assert.Empty(t, r.URL.RawQuery)
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
		assert.Equal(t, "example-backend", r.Header.Get("X-Backend-Id"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "application/json", r.Header.Get("Accept"))
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- request
		_, _ = io.WriteString(w, `{"data":[{"rps":50.125}],"valueFieldHints":["rps"]}`)
	}))
	defer server.Close()
	s := groundcoverTestScaler(t, server, groundcoverTestConfig(server.URL))

	for _, test := range []struct {
		now, start, end string
	}{
		{"2026-11-05T12:03:17Z", "2026-11-05T11:57:00Z", "2026-11-05T12:02:00Z"},
		{"2026-11-05T12:04:17Z", "2026-11-05T11:58:00Z", "2026-11-05T12:03:00Z"},
	} {
		t.Run(test.now, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, test.now)
			require.NoError(t, err)
			s.now = func() time.Time { return now }
			metrics, active, err := s.GetMetricsAndActivity(context.Background(), "s2-groundcover")
			require.NoError(t, err)
			require.Len(t, metrics, 1)
			assert.True(t, active)
			assert.Equal(t, "s2-groundcover", metrics[0].MetricName)
			assert.Equal(t, int64(50125), metrics[0].Value.MilliValue())
			assert.Equal(t, test.end, metrics[0].Timestamp.UTC().Format(time.RFC3339))
			require.NotNil(t, metrics[0].WindowSeconds)
			assert.Equal(t, int64(300), *metrics[0].WindowSeconds)
			assert.Equal(t, map[string]any{
				"start": test.start,
				"end":   test.end,
				"query": groundcoverTestQuery,
				"sources": []any{map[string]any{
					"key": "_time", "origin": "root",
					"filters": []any{map[string]any{"op": "lt", "value": test.end}},
				}},
			}, <-requests)
		})
	}
}

func TestGroundcoverResultContract(t *testing.T) {
	for _, test := range []struct {
		name, response, field, activation string
		wantMilli                         int64
		wantActive                        bool
		wantError                         string
	}{
		{name: "fractional value with inferred field", response: `{"data":[{"rps":0.125}],"valueFieldHints":["rps"]}`, wantMilli: 125, wantActive: true},
		{name: "measured zero", response: `{"data":[{"rps":0}],"valueFieldHints":["rps"]}`},
		{name: "activation threshold equality stays inactive", response: `{"data":[{"rps":2.5}],"valueFieldHints":["rps"]}`, activation: "2.5", wantMilli: 2500},
		{name: "activation threshold exceeded", response: `{"data":[{"rps":2.501}],"valueFieldHints":["rps"]}`, activation: "2.5", wantMilli: 2501, wantActive: true},
		{name: "explicit field disambiguates", response: `{"data":[{"requests":300,"rps":1}],"valueFieldHints":["requests","rps"]}`, field: "rps", wantMilli: 1000, wantActive: true},
		{name: "explicit field needs no hints", response: `{"data":[{"rps":1.5}]}`, field: "rps", wantMilli: 1500, wantActive: true},
		{name: "empty result is not zero", response: `{"data":[],"valueFieldHints":[]}`, wantError: "no data"},
		{name: "missing data envelope", response: `{}`, field: "rps", wantError: "no data"},
		{name: "null is not zero", response: `{"data":[{"rps":null}],"valueFieldHints":["rps"]}`, wantError: "missing or null"},
		{name: "missing selected value", response: `{"data":[{"requests":300}],"valueFieldHints":["requests"]}`, field: "rps", wantError: "missing or null"},
		{name: "numeric string rejected", response: `{"data":[{"rps":"42"}],"valueFieldHints":["rps"]}`, wantError: "JSON number"},
		{name: "boolean rejected", response: `{"data":[{"rps":true}],"valueFieldHints":["rps"]}`, wantError: "JSON number"},
		{name: "negative value rejected", response: `{"data":[{"rps":-1}],"valueFieldHints":["rps"]}`, wantError: "non-negative"},
		{name: "non-finite magnitude rejected", response: `{"data":[{"rps":1e400}],"valueFieldHints":["rps"]}`, wantError: "finite"},
		{name: "multiple rows rejected", response: `{"data":[{"rps":1},{"rps":2}],"valueFieldHints":["rps"]}`, wantError: "2 rows"},
		{name: "ambiguous fields rejected", response: `{"data":[{"requests":300,"rps":1}],"valueFieldHints":["requests","rps"]}`, wantError: "set valueField"},
		{name: "missing hints need explicit field", response: `{"data":[{"rps":1}]}`, wantError: "set valueField"},
		{name: "invalid JSON", response: `not json`, field: "rps", wantError: "invalid groundcover response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, test.response)
			}))
			defer server.Close()
			config := groundcoverTestConfig(server.URL)
			if test.field != "" {
				config.TriggerMetadata["valueField"] = test.field
			}
			if test.activation != "" {
				config.TriggerMetadata["activationThreshold"] = test.activation
			}
			s := groundcoverTestScaler(t, server, config)
			metrics, active, err := s.GetMetricsAndActivity(context.Background(), "s2-groundcover")
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				assert.Empty(t, metrics)
				assert.False(t, active)
				return
			}
			require.NoError(t, err)
			require.Len(t, metrics, 1)
			assert.Equal(t, test.wantMilli, metrics[0].Value.MilliValue())
			assert.Equal(t, test.wantActive, active)
		})
	}
}

func TestGroundcoverHTTPFailures(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "sensitive upstream diagnostic")
			}))
			defer server.Close()
			s := groundcoverTestScaler(t, server, groundcoverTestConfig(server.URL))
			metrics, active, err := s.GetMetricsAndActivity(context.Background(), "s2-groundcover")
			require.ErrorContains(t, err, "HTTP ")
			assert.NotContains(t, err.Error(), "sensitive upstream diagnostic")
			assert.Empty(t, metrics)
			assert.False(t, active)
		})
	}
}

func TestGroundcoverCancellationAndTimeout(t *testing.T) {
	for _, mode := range []string{"cancellation", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			config := groundcoverTestConfig(server.URL)
			if mode == "timeout" {
				config.GlobalHTTPTimeout = 100 * time.Millisecond
			}
			s := groundcoverTestScaler(t, server, config)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancellation" {
				go func() {
					select {
					case <-started:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			metrics, active, err := s.GetMetricsAndActivity(ctx, "s2-groundcover")
			require.Error(t, err)
			if mode == "cancellation" {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected timeout, got %v", err)
			}
			assert.Empty(t, metrics)
			assert.False(t, active)
		})
	}
}

func TestGroundcoverRejectsRedirects(t *testing.T) {
	var redirected atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/unexpected" {
			redirected.Add(1)
			_, _ = io.WriteString(w, `{"data":[{"rps":99}],"valueFieldHints":["rps"]}`)
			return
		}
		http.Redirect(w, r, "/unexpected", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	s := groundcoverTestScaler(t, server, groundcoverTestConfig(server.URL))
	metrics, active, err := s.GetMetricsAndActivity(context.Background(), "s2-groundcover")
	require.ErrorContains(t, err, "HTTP 307")
	assert.Empty(t, metrics)
	assert.False(t, active)
	assert.Zero(t, redirected.Load())
}

func TestGroundcoverRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat(" ", (1<<20)+1))
	}))
	defer server.Close()
	s := groundcoverTestScaler(t, server, groundcoverTestConfig(server.URL))
	metrics, active, err := s.GetMetricsAndActivity(context.Background(), "s2-groundcover")
	require.ErrorContains(t, err, "exceeds 1 MiB")
	assert.Empty(t, metrics)
	assert.False(t, active)
}

func TestGroundcoverConfiguration(t *testing.T) {
	for _, test := range []struct {
		name, key, value string
	}{
		{"HTTP URL", "serverAddress", "http://example.com"},
		{"missing host", "serverAddress", "https:///"},
		{"URL credentials", "serverAddress", "https://user:pass@example.com"},
		{"URL query", "serverAddress", "https://example.com?token=value"},
		{"URL fragment", "serverAddress", "https://example.com#fragment"},
		{"missing backend", "backendId", ""},
		{"blank query", "query", " "},
		{"time override", "query", "resource_type:http is_inbound:true _time:>now-1h | count"},
		{"case insensitive time override", "query", "resource_type:http _TIME:>now-1h | count"},
		{"unsupported lookback", "lookback", "2m"},
		{"invalid duration", "lookback", "later"},
		{"delay too short", "delay", "29s"},
		{"delay too long", "delay", "301s"},
		{"zero threshold", "threshold", "0"},
		{"threshold below metric precision", "threshold", "0.0001"},
		{"negative threshold", "threshold", "-1"},
		{"NaN threshold", "threshold", "NaN"},
		{"infinite threshold", "threshold", "+Inf"},
		{"negative activation threshold", "activationThreshold", "-1"},
		{"infinite activation threshold", "activationThreshold", "+Inf"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := groundcoverTestConfig("https://example.com")
			config.TriggerMetadata[test.key] = test.value
			_, err := NewGroundcoverScaler(config)
			require.Error(t, err)
		})
	}
	t.Run("API key must come from authentication", func(t *testing.T) {
		config := groundcoverTestConfig("https://example.com")
		config.AuthParams = nil
		config.TriggerMetadata["apiKey"] = "inline-key-is-not-accepted"
		_, err := NewGroundcoverScaler(config)
		require.Error(t, err)
	})
	t.Run("blank API key", func(t *testing.T) {
		config := groundcoverTestConfig("https://example.com")
		config.AuthParams["apiKey"] = " "
		_, err := NewGroundcoverScaler(config)
		require.Error(t, err)
	})
	for _, lookback := range []string{"1m", "5m", "10m"} {
		t.Run("valid lookback "+lookback, func(t *testing.T) {
			config := groundcoverTestConfig("https://example.com")
			config.TriggerMetadata["lookback"] = lookback
			scaler, err := NewGroundcoverScaler(config)
			require.NoError(t, err)
			require.NoError(t, scaler.Close(context.Background()))
		})
	}
}

func TestGroundcoverMetricTarget(t *testing.T) {
	scaler, err := NewGroundcoverScaler(groundcoverTestConfig("https://example.com"))
	require.NoError(t, err)
	defer func() { require.NoError(t, scaler.Close(context.Background())) }()
	specs := scaler.GetMetricSpecForScaling(context.Background())
	require.Len(t, specs, 1)
	assert.Equal(t, v2.ExternalMetricSourceType, specs[0].Type)
	require.NotNil(t, specs[0].External)
	assert.Equal(t, "s2-groundcover", specs[0].External.Metric.Name)
	target := specs[0].External.Target
	assert.Equal(t, v2.AverageValueMetricType, target.Type)
	require.NotNil(t, target.AverageValue)
	assert.Equal(t, int64(60000), target.AverageValue.MilliValue())
	assert.Nil(t, target.Value)

	config := groundcoverTestConfig("https://example.com")
	config.TriggerMetadata["threshold"] = "0.001"
	minimumScaler, err := NewGroundcoverScaler(config)
	require.NoError(t, err)
	defer func() { require.NoError(t, minimumScaler.Close(context.Background())) }()
	minimumTarget := minimumScaler.GetMetricSpecForScaling(context.Background())[0].External.Target
	require.NotNil(t, minimumTarget.AverageValue)
	assert.Equal(t, int64(1), minimumTarget.AverageValue.MilliValue())
}

func TestGroundcoverLocalDemo(t *testing.T) {
	manifest, err := os.ReadFile("../../examples/groundcover/scaledobject.yaml")
	require.NoError(t, err)
	var object kedav1alpha1.ScaledObject
	require.NoError(t, yaml.Unmarshal(manifest, &object))
	require.Equal(t, "ScaledObject", object.Kind)
	require.Len(t, object.Spec.Triggers, 1)
	trigger := object.Spec.Triggers[0]
	require.Equal(t, "groundcover", trigger.Type)
	require.NotNil(t, trigger.AuthenticationRef)
	require.NotEmpty(t, trigger.AuthenticationRef.Name)
	requests := make(chan map[string]any, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/apm-measurements/search", r.URL.Path)
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
		assert.Equal(t, trigger.Metadata["backendId"], r.Header.Get("X-Backend-Id"))
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode demo request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- body
		_, _ = io.WriteString(w, `{"data":[{"rps":420}],"valueFieldHints":["rps"]}`)
	}))
	defer server.Close()
	config := groundcoverTestConfig(server.URL)
	config.TriggerIndex = 0
	config.TriggerMetadata = trigger.Metadata
	config.TriggerMetadata["serverAddress"] = server.URL
	config.MetricType = trigger.MetricType
	s := groundcoverTestScaler(t, server, config)
	spec := s.GetMetricSpecForScaling(context.Background())[0]
	require.Equal(t, v2.AverageValueMetricType, spec.External.Target.Type)
	require.Equal(t, int64(60000), spec.External.Target.AverageValue.MilliValue())
	for _, now := range []time.Time{
		time.Date(2026, 11, 5, 12, 3, 17, 0, time.UTC),
		time.Date(2026, 11, 5, 12, 4, 17, 0, time.UTC),
	} {
		s.now = func() time.Time { return now }
		metrics, active, err := s.GetMetricsAndActivity(context.Background(), spec.External.Metric.Name)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		require.Equal(t, int64(420000), metrics[0].Value.MilliValue())
		require.True(t, active)
		body := <-requests
		assert.Equal(t, trigger.Metadata["query"], body["query"])
		t.Logf("poll=%s POST /api/apm-measurements/search window=[%s,%s) mock_rps=420 target_rps_per_replica=60 active=%t", now.Format(time.RFC3339), body["start"], body["end"], active)
	}
	t.Log("Loaded the example ScaledObject and exercised the real scaler against a local TLS mock; no Kubernetes autoscaling or live APM storage was exercised.")
}
