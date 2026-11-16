# SDD: Shared Storage V2, Kubernetes-native secret delivery

Status: draft for review. Tracks the work to replace SMB shared storage in self-managed NVCF with Kubernetes-native backends.

## Goal

Deliver per-function sensitive data and metadata credentials to workloads through
Kubernetes Secrets instead of the in-cluster SMB server and SMB CSI volumes, while
keeping the existing file paths and keeping SMB as the default until deployments
migrate.

## Scope

In scope:

- `/var/secrets` (runtime secrets) and `/var/run/nvcf/info` (metadata credentials).
- The configuration that selects the secret backend, independently of the shared-data
  backend.

Out of scope for this document:

- `/var/task` and model-cache data. They stay on SMB until the RWX backend ships.
- Removing the SMB CSI checks and the samba image default.

## What runs today

NVCA creates SMB-backed volumes for each Helm workload:

| Data | PVCs | Mount path |
|---|---|---|
| Runtime secrets | `nvcf-secrets-data-rw`, `nvcf-secrets-data-ro` | `/var/secrets` |
| Metadata credentials (KNS) | `nvcf-kns-data-rw`, `nvcf-kns-data-ro` | `/var/run/nvcf/info` |
| Task data | `nvcf-task-data-rw` | `/var/task` |

The utility pod mounts the read-write claims and writes into them. The ESS agent
(a fork of HashiCorp consul-template, `src/compute-plane-services/ess-agent`) renders
templates to a filesystem `destination`. The worker writes the metadata token to
`/var/run/nvcf/info/self` using a temporary file and a rename
(`src/libraries/go/worker/nvcf/metadata.go`). Instance pods mount the read-only
claims. The mounts are applied through webhook annotations, and the SMB server image
comes from `agent.sharedStorage.server.image` in the NVCA configuration.

## Design

```mermaid
flowchart LR
  NVCA["NVCA"] -->|"creates"| RS["Secret: runtime"]
  NVCA -->|"creates"| MS["Secret: metadata"]
  NVCA -->|"creates"| SA["ServiceAccount + Role"]
  NVCA -->|"creates"| CM["HCL ConfigMap"]
  U["Utility pod"] --> ESS["ESS agent: k8s_secret sink"]
  U --> W["Worker: metadata refresher"]
  ESS -->|"patch data[key]"| RS
  W -->|"patch data[key]"| MS
  RS -->|"projected, read-only"| I["Instance pod: /var/secrets"]
  MS -->|"projected, read-only"| I2["Instance pod: /var/run/nvcf/info"]
```

### Backend selection

`secretBackend` and `sharedDataBackend` are independent settings:

| `secretBackend` | Behavior |
|---|---|
| unset or `smb` | Existing behavior. This is the default. |
| `k8s-native` | Secrets, ServiceAccount, Role and ConfigMap as described below. |

When the new fields are omitted, behavior is unchanged. Invalid values, incompatible
combinations and an ambiguous default RWX class are rejected at configuration time.
The configuration reaches the agent through `NVCA_SHARED_STORAGE_CONFIG_JSON_BASE64`
and the operator chart values.

### Secret objects

NVCA creates, per function instance, and attaches them to the `StorageRequest` ownership
lifecycle:

- a runtime Secret, holding the rendered runtime data under the key `secrets.json`;
- a metadata Secret, holding the metadata credential under the key `self`;
- a ServiceAccount for the utility workload and a Role that grants only `get` and
  `patch` on those two Secrets through `resourceNames`;
- a ConfigMap with the per-function ESS HCL that names the Secret references;
- an optional NetworkPolicy.

Names are proposed as `<storagerequest>-runtime` and `<storagerequest>-metadata`. This is a
decision to confirm.

Size budgets: 768 KiB decoded per key and 900 KiB decoded for all keys of one Secret.
The Kubernetes API accepts at most 1,048,576 bytes of data per Secret, so the budgets
leave about 124 KiB of headroom. Writers reject a value over budget before calling the API.

### Writers

- **ESS agent.** A template sink, `k8s_secret { name namespace key }`, as an
  alternative to `destination`. Setting both is rejected. It patches only the owned
  `data[key]` with the in-cluster client, treats an unchanged value as a no-op, and
  returns distinct errors for NotFound, Forbidden and retryable API failures.
- **Worker.** When `NVCF_METADATA_CREDENTIALS_SECRET_NAME` and
  `NVCF_METADATA_CREDENTIALS_SECRET_KEY` are set, the metadata refresher patches that Secret
  key. Without them it keeps writing `/var/run/nvcf/info/self`. `ESS_HCL_SOURCE`, when set,
  makes the worker copy that file verbatim into the ESS agent shared configuration
  directory.

### Readers and mounts

- Instance workloads receive the Secrets as **projected volumes, read-only, mounted as whole
  directories**. The files appear at `/var/secrets/secrets.json` and
  `/var/run/nvcf/info/self`, so existing paths stay compatible.
- `subPath` mounts are not allowed. A `subPath` file never receives updates (measured below).
- NVCA applies a typed mount plan, built from `StorageRequest.Status.SharedStorage`, to Pods
  and pod templates in Deployments, ReplicaSets, StatefulSets, Jobs and CronJobs, including
  every container and init container.
- The utility workload receives its HCL ConfigMap, `ESS_HCL_SOURCE` and the writer
  ServiceAccount. It does not mount the read-only sensitive-data Secrets.

## Update propagation

A write reaches the workload only after the kubelet rewrites the projected volume. The
kubelet on a default cluster has `syncFrequency: 1m` and uses the `Watch` change-detection
strategy, so the cache is current almost immediately and the file is rewritten on the pod
sync.

### Measurements

Two experiments on a local k3d compute cluster, 23 updates in total. The API patch itself took
7 to 9 ms.

| Experiment | Updates | Min | Median | Max |
|---|---|---|---|---|
| Patch immediately after the previous update was observed (worst phase), small Secret | 8 | 49 s | 70 s | 77 s |
| Same, Secret near the size budget (about 690 KB) | 3 | 68 s | 77 s | 82 s |
| Writer and reader sidecars in one pod, varied phase | 12 | 19.7 s | 44.8 s | 69.0 s |

Secret size did not change the delay. File replacement is atomic: the kubelet swaps a
`..data` symlink, so a reader never sees a partial file.

### Budget and rules

1. The design budget for Secret-to-file propagation is **120 seconds**.
2. **Consumers must re-read the file on each use.** A process must not cache the content
   for its lifetime.
3. **Initial population.** The Secret key must exist before the workload starts. NVCA
   creates the Secret with the first value, or the workload waits for it. A missing key
   would otherwise leave the file absent until the first sync.
4. **Metadata credentials.** The worker refreshes at 80% of the reported lifetime plus up to
   60 seconds of jitter, retrying every minute after a failure
   (`src/libraries/go/worker/token/token.go`). The new value reaches the workload about
   0.8 x lifetime + jitter + propagation delay after issue. This is safe when
   `0.2 x lifetime - 60 s` exceeds the propagation budget. The server reports a lifetime of
   3 hours, which leaves a margin of about 36 minutes. The token's own `exp` is set by the
   notary service and has not been verified: see Open items. Validation must check the
   real margin before native mode is enabled.

## Security

- The writer Role grants `get` and `patch` on the two named Secrets only. In the experiment,
  patching or reading a different Secret returned 403.
- Sensitive data is stored in etcd. Encryption at rest is a cluster setting. Non-local
  clusters need evidence that it is enabled. A local functional test does not
  validate this control.
- Values are not logged. Secrets are owned by the `StorageRequest` and removed with it.

## Coexistence and migration

- SMB stays the default. Settings that omit the new fields behave as before.
- Existing `StorageRequest`s keep their pinned backend. A change to the settings affects
  new requests.
- `secretBackend` and `sharedDataBackend` are independent, so `k8s-native` secrets can run
  with SMB-backed task data and model cache.
- Rollback returns `secretBackend` to `smb`. The rollout guide describes it for both
  SMB backends.
- Mixed versions need a defined behavior, for example a new ESS agent with an older NVCA. The
  ESS agent keeps supporting `destination`, so an older NVCA is unaffected.

## Known limitation and workaround

Updates take up to about 80 seconds to appear. For deployments that need faster
propagation, the cluster operator can lower the kubelet `syncFrequency`. That setting is
node-level, adds sync load, and was not tested for this document, so treat it as optional
tuning and verify it on your own cluster.

## Validation

- `nvcf_shared_storage_cold_start_seconds` with the labels `secret_backend`,
  `shared_data_backend` and `smb_server_expected`.
- Tests for projected-file latency, Secret updates, metadata updates and RBAC.
- A fully SMB-free configuration also needs the RWX backend and the SMB CSI changes.

## Open items

- Verify the real `exp` of the signed metadata token on a running function worker.
- Audit which components read `/var/run/nvcf/info/self` and `/var/secrets/secrets.json`, and
  confirm that none cache the content.
- Confirm the Secret naming scheme.
- Decide how a workload waits for initial population.
- Collect encryption-at-rest evidence for a non-local cluster.
- Define mixed-version behavior across ESS agent, worker and NVCA.

## Source references

- `src/compute-plane-services/nvca/pkg/storage/sharedstorage.go`, `types.go`
- `src/compute-plane-services/nvca/internal/miniservice/reconcile_storagerequests.go`
- `src/compute-plane-services/ess-agent`
- `src/libraries/go/worker/nvcf/metadata.go`, `src/libraries/go/worker/token/token.go`
- `src/control-plane-services/cloud-functions/nvcf-core/src/main/java/com/nvidia/nvcf/grpc/GrpcWorkerService.java`
