# Object-store session reports

User-facing reference for the `objectStore` sink on
`PodTraceSession.spec.reportRef`.

## What it does

When `spec.reportRef.objectStore` is set, the operator attaches a native
sidecar (`report-uploader`) to each session Job. After the main session
container finishes writing `/var/run/podtrace/report.txt`, the sidecar
uploads it (and the matching `summary.json`) to the cloud object store
named by `spec.reportRef.objectStore.uri`.

Once the upload succeeds, the operator surfaces:

- `status.reportLocation` — the resolved object URI, e.g.
  `s3://my-bucket/reports/diag-abc-2026-05-13T12-34-56Z.txt`
  (single-node sessions only).
- `status.conditions[type=ReportUploaded]` —
  - `True` (`ObjectStoreUploadSucceeded`) when the upload landed.
  - `False` (`ObjectStoreUploadFailed`) when the sidecar exited non-zero;
    `message` carries the sidecar's stderr.
  - `Unknown` (`UploadPending`) while the sidecar is still running.

## Multi-node sessions

A session fans out one Job per node, and each node uploads its own
report independently. The session-level result is therefore tracked
**per node** in `status.nodeReports` (map keyed by `node`):

- `state` — `Pending` until that node's uploader terminates, then
  `Succeeded` or `Failed`; terminal entries are sticky and survive
  uploader Pod TTL garbage-collection and operator restarts.
- `location` — the resolved object URI for a succeeded node.
- `failureReason` — the stable failure enum for a failed node, with the
  free-text error in `message`.
- `attempts` — the highest uploader restart count observed for the node.

The `ReportUploaded` condition converges only after **every** node the
session originally fanned out to is `Succeeded` or `Failed`: all
succeeded gives `True`, any failure gives `False` (partial failures list
both the succeeded locations and the failed reasons in the condition
message). A node finishing early never decides the session while other
uploaders are still running, and the outcome does not depend on Pod
list ordering.

The session's `ttlSecondsAfterFinished` countdown cannot delete the
session while `ReportUploaded` is still `Unknown`: the uploader Pods are
the only evidence for nodes that have not concluded, so cleanup waits
until the aggregate result converges. The operator keeps reconciling
uploaders after the session itself is `Completed`/`Failed`; a finished
Job does not imply a converged report. After an operator restart the
same conclusion is reconstructed from live Pods plus the persisted
`status.nodeReports`.

Single-node sessions continue to populate the scalar
`status.reportLocation`, `status.reportFailureReason`, and
`status.reportAttempts` fields as before.

## When to use

| Sink | Best for | Max size |
|---|---|---|
| `configMap` | small text reports a human will copy/paste | ~1 MiB (etcd limit) |
| `secret` | reports containing sensitive paths or payloads | ~1 MiB (etcd limit) |
| `objectStore` | large reports, archival, downstream pipelines | bucket-dependent (TB-scale) |

The three sinks are mutually exclusive — set at most one.

## Cluster requirements

- **Kubernetes 1.29+**. The sidecar uses the native sidecar pattern
  (initContainer with `restartPolicy: Always`). Older clusters will
  silently treat the sidecar as a regular initContainer, which exits
  before the main session starts and breaks the upload.
- `TracerConfig.spec.session.sidecarUploader: true`. Off by default
  because most clusters have no destination bucket. Flip via Helm:
  `--set tracerConfig.sidecarUploader=true`.

## URI scheme

```
s3://<bucket>/<key-or-prefix>
gs://<bucket>/<key-or-prefix>
azblob://<storage-account>/<container>/<key-or-prefix>
```

A **trailing slash** means "prefix mode" — the uploader picks the
object filename:

| URI | Resolves to (example) |
|---|---|
| `s3://b/reports/` | `s3://b/reports/<pod>-2026-05-13T12-34-56Z.txt` |
| `s3://b/reports/fixed.txt` | `s3://b/reports/fixed.txt` |

In prefix mode the uploader **also** writes a second object:
`<key>.summary.json` carrying the same compact summary that lands on
`status.summary`. Use prefix mode unless you have a downstream pipeline
that needs a fixed key (and accepts that consecutive sessions will
overwrite it).

## Credentials

The uploader prefers **ambient credentials** — IRSA on EKS, Workload
Identity on GKE, Managed Identity on AKS. Don't set
`credentialsSecretRef` if any of these is wired; the SDKs will discover
credentials automatically.

For clusters without ambient credentials, supply a Secret in the
session's namespace and reference it from `credentialsSecretRef.name`.
The Secret's keys are read by the backend per the schema below.

### S3 (`s3://`)

| Key | Required when | Notes |
|---|---|---|
| `access_key_id` | no IRSA | static AWS access key |
| `secret_access_key` | no IRSA | static AWS secret |
| `session_token` | optional | STS session tokens |
| `region` | optional | defaults to `AWS_REGION` env, then `us-east-1` |
| `endpoint` | S3-compatible (R2, Wasabi, B2, SeaweedFS) | URL of the non-AWS endpoint |
| `force_path_style` | usually with `endpoint` | set to `"true"` for path-style addressing |

### GCS (`gs://`)

| Key | Required when | Notes |
|---|---|---|
| `service_account_json` | no Workload Identity | full SA key JSON (`gcloud iam service-accounts keys create`) |
| `endpoint` | testing only | for fake-gcs-server / dev clusters |

### Azure Blob (`azblob://`)

| Key | Required when | Notes |
|---|---|---|
| `account_key` | shared-key auth | preferred for local-dev / Azurite |
| `tenant_id` / `client_id` / `client_secret` | SPN auth | all three required |
| `endpoint` | testing only | overrides the `https://<account>.blob.core.windows.net` default |

Leave every key absent to fall back to `DefaultAzureCredential`
(Managed Identity, env vars, CLI auth).

## Example: S3 with explicit credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: backup-s3-creds
  namespace: my-app
type: Opaque
stringData:
  access_key_id: AKIA...
  secret_access_key: ...
  region: eu-west-1
---
apiVersion: podtrace.io/v1alpha1
kind: PodTraceSession
metadata:
  name: nightly-trace
  namespace: my-app
spec:
  selector:
    matchLabels: { app: api }
  duration: 60s
  exporterRef:
    name: prod-otlp
  reportRef:
    objectStore:
      uri: s3://podtrace-reports/diagnose/
      credentialsSecretRef:
        name: backup-s3-creds
```

After the session completes:

```bash
kubectl -n my-app get pts nightly-trace \
  -o jsonpath='{.status.reportLocation}'
# s3://podtrace-reports/diagnose/nightly-trace-...-2026-05-13T12-34-56Z.txt
```

## Example: GCS with Workload Identity (no Secret)

```yaml
apiVersion: podtrace.io/v1alpha1
kind: PodTraceSession
metadata:
  name: gcs-trace
  namespace: my-app
spec:
  selector:
    matchLabels: { app: api }
  duration: 60s
  exporterRef:
    name: prod-otlp
  reportRef:
    objectStore:
      uri: gs://podtrace-reports/diagnose/
      # No credentialsSecretRef — Workload Identity supplies creds.
```

The session pod's ServiceAccount must be bound to a GCP SA with object
write permission on the bucket. The standard Workload Identity binding
pattern applies; see GKE docs.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `ReportUploaded=False`, message contains `dial tcp: lookup ... no such host` | bucket name typo or wrong region |
| `ReportUploaded=False`, message contains `AccessDenied` | credentials are valid but lack `s3:PutObject` (or equivalent) on the bucket |
| `ReportUploaded=Unknown` indefinitely | cluster is <1.29 — the sidecar isn't getting native lifecycle semantics; upgrade or flip `tracerConfig.sidecarUploader=false` and use a ConfigMap sink instead |

## Security considerations

- The uploader sidecar runs with the **session pod's** ServiceAccount.
  It does NOT need K8s API access — it reads files from the shared
  rundir and uploads via HTTPS to the cloud.
- The credentials Secret is projected into the sidecar only — the main
  session container cannot see it.
- Object URIs are stored verbatim on the CR; do not embed secrets in
  the URI (use the Secret).