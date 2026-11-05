# groundcover scaler

The `groundcover` trigger scales a workload from a gcQL APM query result. The
query defines the measurement and its units; the scaler handles authentication,
the moving time window and the numeric result. It calls the existing APM search
API with POST.

This draft requires a KEDA build containing the scaler. It is not available in
released KEDA.

## Configuration

Use [scaledobject.yaml](scaledobject.yaml) with
[authentication.yaml](authentication.yaml). Create a Secret named
`groundcover-api` in the same namespace, with an `apiKey` key containing a
groundcover service-account API key with APM read access. Omit the `Bearer `
prefix. The `TriggerAuthentication` resource reads this key from the Secret.

Required metadata is `serverAddress`, `backendId`, `query` and `threshold`.
Use an HTTPS base URL for `serverAddress`. The query must return one row with
one finite, non-negative numeric result. If it returns several numeric fields,
select one with `valueField`; otherwise the scaler uses the API's single numeric
field hint. The threshold uses the same units as the query result.

| Optional metadata | Default | Meaning |
| --- | --- | --- |
| `lookback` | `5m` | APM window length. Accepts `1m`, `5m`, `10m`. |
| `delay` | `1m` | Allowance for ingestion before querying the window. Accepts `30s` to `5m`. |
| `valueField` | Auto-select | Output alias to use when several numeric fields are returned. |
| `activationThreshold` | `0` | The scaler reports active when the result exceeds this value. |

The example computes average requests per second over five minutes, with a
target of 60 requests per second per replica. Set the filters, threshold and
replica limits for your workload. If you change the lookback, update the query's
divisor too. The scaler does not infer units or rewrite the gcQL query.

KEDA uses milli-unit precision. The threshold must be at least `0.001`. Return
small latency values in milliseconds to avoid rounding the metric to zero.

## Window and missing data

Each poll calculates `end = floor_UTC(now - delay, 1m)` and
`start = end - lookback`. At `12:03:17Z`, the defaults request
`[11:57:00Z, 12:02:00Z)`. One minute later, the window advances to
`[11:58:00Z, 12:03:00Z)`. The metric timestamp is the window end.

The search API includes its end timestamp, so the scaler adds a source filter
for `_time < end`. Live API validation of this strict boundary is still required.
Queries that reference `_time` are rejected to prevent time overrides. The
conservative check also rejects that token inside quoted text.

Keep `stats by(workload)` and the workload filter in the example so an empty
window produces no grouped rows. An ungrouped aggregate or zero-fill expression
can produce zero without measurements. The scaler cannot distinguish that result
from a measured zero and does not enforce grouping in arbitrary gcQL.

Empty results, null values, multiple rows, ambiguous fields and invalid numbers
return errors. Numeric zero is accepted. HTTP failures, timeouts and cancellation
also return errors; an older window is never substituted. KEDA's fallback and
HPA behavior still apply.

The delay does not guarantee complete data. Late or dropped measurements can
undercount traffic. APM alone cannot wake a workload from zero when no telemetry
arrives. These short lookbacks query raw APM data; polling cost and Kubernetes
scaling behavior still require end-to-end validation.

## Tests and demo

From the KEDA checkout root, run the scaler tests in the full package:

```sh
go test ./pkg/scalers -run '^TestGroundcover' -count=1
```

The focused script compiles the scaler with KEDA's actual interface and helpers:

```sh
./examples/groundcover/test.sh
./examples/groundcover/test.sh -run '^TestGroundcoverLocalDemo$'
```

The demo loads the example YAML and sends authenticated requests to a mock HTTPS
API. It verifies the client contract, not live APM storage or Kubernetes scaling.
