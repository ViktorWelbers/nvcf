// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Proxy-level coverage for routing reservations created per dispatch attempt.
//!
//! Requests run through `proxy_openai_request` against a minimal raw QUIC
//! tunnel that plays the Pylon side, so tests observe the
//! `x-stargate-expected-queue-ms` header exactly as Stargate sent it and can
//! return arbitrary tunnel retry metadata.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::IntoResponse;
use parking_lot::Mutex;
use stargate_proto::pb::{
    InferenceServerModelRegistration, InferenceServerRegistration, InferenceServerStatus,
    ModelStats,
};
use stargate_protocol::tunnel_contract::{
    HEADER_INFERENCE_SERVER_ID, HEADER_INPUT_TOKENS, HEADER_MODEL, HEADER_REQUEST_ID,
    HEADER_STARGATE_EXPECTED_QUEUE_MS, HEADER_STARGATE_RETRY_REASON, HEADER_STARGATE_RETRYABLE,
};
use stargate_protocol::{RecvStream, SendStream, TunnelTransportProtocol};

use super::test_support::test_proxy_app_state;
use super::{OpenAiProxyEndpoint, ProxyAppState, proxy_openai_request};
use crate::auth::OpenAuthenticator;
use crate::routing_state::{RegistrationIdentity, RoutingTargetKey, RunningRegistration};
use crate::tunnel::{QuicHttpProxy, QuicTunnelConfig};

const MODEL: &str = "model-reservation";
const CLUSTER: &str = "cluster-reservation";
const INPUT_TOKENS: u64 = 37;
const BASE_QUEUE_MS: u64 = 5;
const LAST_MEAN_INPUT_TPS: f64 = 100.0;
/// Queue time one reservation adds: 37 tokens at 100 tokens/s.
const RESERVATION_QUEUE_MS: u64 = 370;
/// Long enough that no reservation reaches its soft expiry while a test runs.
const UNEXPIRING_TTL: Duration = Duration::from_secs(600);

#[derive(Clone, Copy)]
enum MockReply {
    Respond {
        status: u16,
        headers: &'static [(&'static str, &'static str)],
    },
    ResetStream,
    /// Pylon's admission rule: reject with a queue mismatch only when the
    /// request carries an expected queue time, otherwise answer 200.
    MismatchWhenEstimated,
}

const OK_REPLY: MockReply = MockReply::Respond {
    status: 200,
    headers: &[],
};

const QUEUE_MISMATCH_REPLY: MockReply = MockReply::Respond {
    status: 429,
    headers: &[
        (HEADER_STARGATE_RETRYABLE, "true"),
        (HEADER_STARGATE_RETRY_REASON, "queue_estimate_mismatch"),
    ],
};

/// Raw QUIC tunnel endpoint standing in for Pylon. It records every request
/// header block Stargate sends and answers with a fixed reply.
struct MockTunnel {
    endpoint: quinn::Endpoint,
    received: Arc<Mutex<Vec<HeaderMap>>>,
}

impl MockTunnel {
    fn start(reply: MockReply) -> Self {
        Self::start_with_stream_limit(reply, None)
    }

    /// Accepts connections but grants no request streams, so Stargate's
    /// stream open blocks until it times out or is cancelled.
    fn start_without_stream_capacity() -> Self {
        Self::start_with_stream_limit(OK_REPLY, Some(0))
    }

    fn start_with_stream_limit(reply: MockReply, max_bidi_streams: Option<u32>) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut server_config = stargate_tls::build_quic_server_config(
            &stargate_tls::ServerTlsIdentity::SelfSigned,
            TunnelTransportProtocol::RawQuic.alpn_protocols(),
        )
        .expect("mock tunnel server config should build");
        if let Some(max_bidi_streams) = max_bidi_streams {
            let mut transport = quinn::TransportConfig::default();
            transport.max_concurrent_bidi_streams(max_bidi_streams.into());
            server_config.transport_config(Arc::new(transport));
        }
        let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap())
            .expect("mock tunnel endpoint should bind");
        let received = Arc::new(Mutex::new(Vec::new()));
        let accept_endpoint = endpoint.clone();
        let accept_received = Arc::clone(&received);
        tokio::spawn(async move {
            while let Some(incoming) = accept_endpoint.accept().await {
                let received = Arc::clone(&accept_received);
                tokio::spawn(async move {
                    let Ok(connection) = incoming.await else {
                        return;
                    };
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        tokio::spawn(serve_stream(send, recv, reply, Arc::clone(&received)));
                    }
                });
            }
        });
        Self { endpoint, received }
    }

    fn url(&self) -> String {
        format!(
            "quic://{}",
            self.endpoint
                .local_addr()
                .expect("mock tunnel should have a local address")
        )
    }

    fn request_count(&self) -> usize {
        self.received.lock().len()
    }

    fn expected_queue_ms_headers(&self) -> Vec<Option<u64>> {
        self.received
            .lock()
            .iter()
            .map(|headers| {
                headers
                    .get(HEADER_STARGATE_EXPECTED_QUEUE_MS)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok())
            })
            .collect()
    }
}

impl Drop for MockTunnel {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"test complete");
    }
}

async fn serve_stream(
    mut quinn_send: quinn::SendStream,
    quinn_recv: quinn::RecvStream,
    reply: MockReply,
    received: Arc<Mutex<Vec<HeaderMap>>>,
) {
    let mut recv = RecvStream::new(quinn_recv);
    let Ok(request_headers) = recv.recv_header().await else {
        return;
    };
    let reply = match reply {
        MockReply::MismatchWhenEstimated
            if request_headers.contains_key(HEADER_STARGATE_EXPECTED_QUEUE_MS) =>
        {
            QUEUE_MISMATCH_REPLY
        }
        MockReply::MismatchWhenEstimated => OK_REPLY,
        reply => reply,
    };
    received.lock().push(request_headers);
    let (status, extra_headers) = match reply {
        MockReply::ResetStream => {
            let _ = quinn_send.reset(0u32.into());
            return;
        }
        MockReply::Respond { status, headers } => (status, headers),
        MockReply::MismatchWhenEstimated => unreachable!("resolved from the request headers"),
    };
    while let Ok(frame) = recv.recv_body().await {
        if frame.into_body().is_none() {
            break;
        }
    }
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("x-status"),
        HeaderValue::from(status),
    );
    for (name, value) in extra_headers {
        response_headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    let mut send = SendStream::new(quinn_send);
    if send.send_header(response_headers).await.is_ok()
        && send
            .send_body(bytes::Bytes::from_static(b"{}"))
            .await
            .is_ok()
    {
        let _ = send.finish();
    }
}

fn proxy_app(ttl_min: Duration, ttl_max: Duration) -> ProxyAppState {
    proxy_app_with_request_timeout(ttl_min, ttl_max, Duration::from_secs(5))
}

fn proxy_app_with_request_timeout(
    ttl_min: Duration,
    ttl_max: Duration,
    request_timeout: Duration,
) -> ProxyAppState {
    let mut app = test_proxy_app_state();
    // The shared fixture uses 10 ms tunnel timeouts because it never dials a
    // backend; these tests complete real QUIC requests.
    app.quic_proxy = Arc::new(
        QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout,
                direct_quic_connections: 1,
                tls_cert_pem: None,
                server_tls_identity: stargate_tls::ServerTlsIdentity::SelfSigned,
                server_identity_reloader: None,
                tls_reload_interval: stargate_tls::DEFAULT_TLS_RELOAD_INTERVAL,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::RawQuic,
            },
            Arc::new(OpenAuthenticator),
        )
        .expect("quic proxy should initialize"),
    );
    app.routing_reservation_ttl_min = ttl_min;
    app.routing_reservation_ttl_max = ttl_max;
    app
}

fn target() -> RoutingTargetKey {
    RoutingTargetKey::new(None, MODEL)
}

/// Registers one active direct backend in the shared test cluster.
async fn register_backend(
    app: &ProxyAppState,
    inference_server_id: &str,
    tunnel: &MockTunnel,
    rtt: Duration,
) -> RunningRegistration {
    register_backend_in(app, CLUSTER, inference_server_id, tunnel, rtt).await
}

/// Registers one active direct backend in `cluster_id`.
async fn register_backend_in(
    app: &ProxyAppState,
    cluster_id: &str,
    inference_server_id: &str,
    tunnel: &MockTunnel,
    rtt: Duration,
) -> RunningRegistration {
    let running = app
        .state
        .begin_registration(&RegistrationIdentity {
            inference_server_id: inference_server_id.to_string(),
            cluster_id: cluster_id.to_string(),
            inference_server_url: tunnel.url(),
            routing_key: None,
            reverse_tunnel: false,
        })
        .expect("registration should begin");
    report_idle(app, &running, rtt).await;
    running
}

/// Applies a registration update reporting an empty queue.
async fn report_idle(app: &ProxyAppState, running: &RunningRegistration, rtt: Duration) {
    let update = InferenceServerRegistration {
        models: HashMap::from([(
            MODEL.to_string(),
            InferenceServerModelRegistration {
                status: InferenceServerStatus::Active.into(),
                stats: Some(ModelStats {
                    last_mean_input_tps: LAST_MEAN_INPUT_TPS,
                    queue_time_estimate_ms_by_priority: HashMap::from([(0, BASE_QUEUE_MS)]),
                    ..Default::default()
                }),
            },
        )]),
        ..Default::default()
    };
    app.state
        .apply_registration_update(running, &update, false, Some(rtt))
        .await;
}

async fn send_request(app: &ProxyAppState, request_id: &str) -> Result<HeaderMap, StatusCode> {
    let (status, headers, _) = send_request_with_headers(app, request_id, &[]).await;
    if status.is_success() {
        Ok(headers)
    } else {
        Err(status)
    }
}

/// Sends a chat request with extra headers and reads the whole response body.
async fn send_request_with_headers(
    app: &ProxyAppState,
    request_id: &str,
    extra_headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, bytes::Bytes) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(OpenAiProxyEndpoint::CHAT_COMPLETIONS.path)
        .header(HEADER_REQUEST_ID, request_id)
        .header(HEADER_MODEL, MODEL)
        .header(HEADER_INPUT_TOKENS, INPUT_TOKENS.to_string());
    for (name, value) in extra_headers {
        request = request.header(*name, *value);
    }
    let request = request
        .body(Body::from("{}"))
        .expect("request should build");
    let response =
        proxy_openai_request(app.clone(), request, OpenAiProxyEndpoint::CHAT_COMPLETIONS)
            .await
            .into_response();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("response body should complete");
    (status, headers, body)
}

/// Reservations currently counted in the routed cluster snapshot. Base stats
/// report an empty queue, so every queued entry is a live reservation.
async fn counted_reservations(app: &ProxyAppState) -> u64 {
    let snapshot = app
        .state
        .routing_target_snapshot(&target())
        .await
        .expect("target should stay routable");
    let clusters = snapshot.clusters();
    assert_eq!(clusters.len(), 1);
    clusters[0].stats.queue_size
}

/// Polls until the routed cluster counts `expected` reservations.
async fn wait_for_counted_reservations(app: &ProxyAppState, expected: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while counted_reservations(app).await != expected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "routed cluster never counted {expected} reservations"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn observed_ttl_sum_ms(app: &ProxyAppState) -> u64 {
    let sum_seconds = app
        .metrics
        .routing_reservation_ttl_seconds(None, MODEL)
        .get_sample_sum();
    (sum_seconds * 1000.0).round() as u64
}

#[tokio::test]
async fn reservation_ttl_uses_the_dispatched_backend_rtt_not_the_cluster_mean() {
    let app = proxy_app(Duration::from_millis(1), Duration::from_millis(1000));
    let fast_tunnel = MockTunnel::start(OK_REPLY);
    let slow_tunnel = MockTunnel::start(OK_REPLY);
    let rtts = HashMap::from([
        ("inst-fast", Duration::from_millis(10)),
        ("inst-slow", Duration::from_millis(100)),
    ]);
    let fast = register_backend(&app, "inst-fast", &fast_tunnel, rtts["inst-fast"]).await;
    let slow = register_backend(&app, "inst-slow", &slow_tunnel, rtts["inst-slow"]).await;
    let cluster_rtt = app
        .state
        .routing_target_snapshot(&target())
        .await
        .expect("target should be routable")
        .clusters()[0]
        .rtt;
    assert_eq!(
        cluster_rtt,
        Duration::from_millis(55),
        "the cluster mean must differ from both backend RTTs"
    );

    // Round robin dispatches one request to each backend.
    let mut dispatched = Vec::new();
    for request_id in ["req-ttl-1", "req-ttl-2"] {
        let ttl_sum_before_ms = observed_ttl_sum_ms(&app);
        let headers = send_request(&app, request_id)
            .await
            .expect("request should be proxied");
        let backend = headers[HEADER_INFERENCE_SERVER_ID]
            .to_str()
            .expect("backend id should be ASCII")
            .to_string();
        let expected_ttl = rtts[backend.as_str()];
        assert_eq!(
            observed_ttl_sum_ms(&app) - ttl_sum_before_ms,
            expected_ttl.as_millis() as u64,
            "reservation for {backend} must use its own RTT, not the {cluster_rtt:?} cluster mean"
        );
        dispatched.push(backend);
    }
    dispatched.sort();
    assert_eq!(dispatched, ["inst-fast", "inst-slow"]);
    assert_eq!(
        app.metrics
            .routing_reservation_ttl_seconds(None, MODEL)
            .get_sample_count(),
        2
    );
    assert_eq!(fast_tunnel.request_count(), 1);
    assert_eq!(slow_tunnel.request_count(), 1);

    app.state.end_registration(fast).await;
    app.state.end_registration(slow).await;
}

#[tokio::test]
async fn unexpired_reservation_raises_expected_queue_header_for_later_requests() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let tunnel = MockTunnel::start(OK_REPLY);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    for request_id in ["req-queue-1", "req-queue-2", "req-queue-3"] {
        send_request(&app, request_id)
            .await
            .expect("request should be proxied");
    }

    assert_eq!(
        tunnel.expected_queue_ms_headers(),
        [
            Some(BASE_QUEUE_MS),
            Some(BASE_QUEUE_MS + RESERVATION_QUEUE_MS),
            Some(BASE_QUEUE_MS + 2 * RESERVATION_QUEUE_MS),
        ],
        "each completed dispatch stays reserved until its backend reports"
    );
    assert_eq!(counted_reservations(&app).await, 3);

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn reported_reservation_is_excluded_from_expected_queue_header() {
    // A zero TTL puts the soft expiry at the dispatch instant. `Instant` is
    // monotonic, so the backend update applied after each request is received
    // at or past the soft expiry without sleeping or controlling the clock.
    let app = proxy_app(Duration::ZERO, Duration::ZERO);
    let tunnel = MockTunnel::start(OK_REPLY);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    for request_id in ["req-expired-1", "req-expired-2"] {
        send_request(&app, request_id)
            .await
            .expect("request should be proxied");
        report_idle(&app, &running, Duration::from_millis(10)).await;
    }

    assert_eq!(
        tunnel.expected_queue_ms_headers(),
        [Some(BASE_QUEUE_MS), Some(BASE_QUEUE_MS)],
        "a reservation settled by its backend's update must not inflate the next estimate"
    );
    assert_eq!(counted_reservations(&app).await, 0);
    assert_eq!(
        app.metrics
            .routing_reservation_ttl_seconds(None, MODEL)
            .get_sample_count(),
        2,
        "both dispatches must have reserved before settling"
    );

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn soft_expired_reservation_counts_until_the_backend_reports() {
    // A zero RTT and TTL put the soft expiry at the dispatch instant, so only
    // the missing registration update keeps the reservation counted.
    let app = proxy_app(Duration::ZERO, Duration::ZERO);
    let tunnel = MockTunnel::start(OK_REPLY);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::ZERO).await;

    for request_id in ["req-silent-1", "req-silent-2"] {
        send_request(&app, request_id)
            .await
            .expect("request should be proxied");
    }

    assert_eq!(
        tunnel.expected_queue_ms_headers(),
        [
            Some(BASE_QUEUE_MS),
            Some(BASE_QUEUE_MS + RESERVATION_QUEUE_MS)
        ],
        "a soft-expired reservation must count until its backend reports"
    );
    assert_eq!(counted_reservations(&app).await, 2);

    report_idle(&app, &running, Duration::ZERO).await;
    assert_eq!(
        counted_reservations(&app).await,
        0,
        "an update after the soft expiry replaces reservations with reported stats"
    );

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn blocked_stream_open_keeps_reservation_unarmed_until_cancelled() {
    // A zero TTL armed at dispatch would end on the first update below.
    let app = proxy_app(Duration::ZERO, Duration::ZERO);
    let tunnel = MockTunnel::start_without_stream_capacity();
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    let request = tokio::spawn({
        let app = app.clone();
        async move { send_request(&app, "req-blocked").await }
    });
    wait_for_counted_reservations(&app, 1).await;
    report_idle(&app, &running, Duration::from_millis(10)).await;
    assert_eq!(
        counted_reservations(&app).await,
        1,
        "an update cannot settle a request still waiting for a stream"
    );

    request.abort();
    assert!(
        request
            .await
            .expect_err("aborted request should not finish")
            .is_cancelled()
    );
    report_idle(&app, &running, Duration::from_millis(10)).await;
    assert_eq!(
        counted_reservations(&app).await,
        0,
        "cancellation must arm the reservation so the next update settles it"
    );
    assert_eq!(tunnel.request_count(), 0);

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn failed_stream_open_arms_reservation_when_the_attempt_ends() {
    let app =
        proxy_app_with_request_timeout(Duration::ZERO, Duration::ZERO, Duration::from_millis(100));
    let tunnel = MockTunnel::start_without_stream_capacity();
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    send_request(&app, "req-open-timeout")
        .await
        .expect_err("a request without stream capacity should fail");
    assert!(
        counted_reservations(&app).await >= 1,
        "failed attempts stay reserved until their backend reports"
    );
    report_idle(&app, &running, Duration::from_millis(10)).await;
    assert_eq!(
        counted_reservations(&app).await,
        0,
        "a failed attempt must arm its reservation so the next update settles it"
    );
    assert_eq!(tunnel.request_count(), 0);

    app.state.end_registration(running).await;
}

async fn assert_reply_keeps_reservations(reply: MockReply, case: &str) {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let tunnel = MockTunnel::start(reply);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    let _ = send_request(&app, "req-failure").await;

    let attempts = tunnel.request_count();
    assert!(
        attempts >= 1,
        "{case}: the backend should receive a dispatch"
    );
    assert!(
        tunnel
            .expected_queue_ms_headers()
            .iter()
            .all(Option::is_some),
        "{case}: only a queue-mismatch rejection may resend without an estimate"
    );
    assert_eq!(
        counted_reservations(&app).await,
        attempts as u64,
        "{case}: every failed attempt must stay reserved until its backend reports"
    );
    let _ = send_request(&app, "req-follow-up").await;
    assert_eq!(
        tunnel.expected_queue_ms_headers().get(attempts).copied(),
        Some(Some(BASE_QUEUE_MS + attempts as u64 * RESERVATION_QUEUE_MS)),
        "{case}: the next request must still see every unreleased reservation"
    );

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn non_queue_mismatch_failures_keep_reservation_until_ttl() {
    for (reply, case) in [
        (
            MockReply::Respond {
                status: 503,
                headers: &[],
            },
            "503 without retry metadata",
        ),
        (
            MockReply::Respond {
                status: 503,
                headers: &[
                    (HEADER_STARGATE_RETRYABLE, "true"),
                    (HEADER_STARGATE_RETRY_REASON, "upstream_admission_rejected"),
                ],
            },
            "retryable 503",
        ),
        (
            MockReply::Respond {
                status: 429,
                headers: &[],
            },
            "429 without retry metadata",
        ),
        (
            MockReply::Respond {
                status: 429,
                headers: &[
                    (HEADER_STARGATE_RETRYABLE, "true"),
                    (HEADER_STARGATE_RETRY_REASON, "upstream_admission_rejected"),
                ],
            },
            "retryable 429 with another reason",
        ),
        (MockReply::ResetStream, "transport failure"),
    ] {
        assert_reply_keeps_reservations(reply, case).await;
    }
}

#[tokio::test]
async fn queue_mismatch_rejection_releases_reservation_before_ttl() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let tunnel = MockTunnel::start(QUEUE_MISMATCH_REPLY);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    for request_id in ["req-mismatch-1", "req-mismatch-2"] {
        let _ = send_request(&app, request_id).await;
    }

    assert_eq!(counted_reservations(&app).await, 0);
    // Each request is resent once without an estimate to the only backend.
    assert_eq!(
        tunnel.expected_queue_ms_headers(),
        [Some(BASE_QUEUE_MS), None, Some(BASE_QUEUE_MS), None],
        "a released reservation must not inflate the next estimate"
    );

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn queue_mismatch_on_the_only_backend_resends_without_an_estimate() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let tunnel = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    let (status, _, body) = send_request_with_headers(&app, "req-last-backend", &[]).await;

    assert_eq!(
        status,
        StatusCode::OK,
        "Pylon admits the resend that carries no estimate"
    );
    assert_eq!(&body[..], b"{}", "the admitted response body completes");
    assert_eq!(
        tunnel.expected_queue_ms_headers(),
        [Some(BASE_QUEUE_MS), None]
    );
    assert_eq!(
        counted_reservations(&app).await,
        1,
        "the admitted resend stays reserved until the backend reports"
    );
    let retries = |reason| app.metrics.proxy_retries_total(None, MODEL, reason).get();
    assert_eq!(retries("queue_mismatch_resend_without_estimate"), 1);
    assert_eq!(
        retries("queue_estimate_mismatch"),
        0,
        "the resend is counted under its own reason"
    );

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn queue_mismatch_is_final_when_the_retry_budget_is_spent() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let tunnel = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    let (status, _, _) =
        send_request_with_headers(&app, "req-no-budget", &[("x-stargate-max-wait-ms", "0")]).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(tunnel.expected_queue_ms_headers(), [Some(BASE_QUEUE_MS)]);

    app.state.end_registration(running).await;
}

/// Asserts that one backend saw a single attempt with an estimate and the
/// other saw an attempt with an estimate and then the resend without one.
fn assert_resent_only_to_the_last_backend(first: &MockTunnel, second: &MockTunnel) {
    let carried_estimate = |tunnel: &MockTunnel| -> Vec<bool> {
        tunnel
            .expected_queue_ms_headers()
            .iter()
            .map(Option::is_some)
            .collect()
    };
    let mut seen = [carried_estimate(first), carried_estimate(second)];
    seen.sort_by_key(Vec::len);
    assert_eq!(
        seen,
        [vec![true], vec![true, false]],
        "each backend is tried with an estimate before the last one is resent without it"
    );
}

#[tokio::test]
async fn queue_mismatch_resends_to_the_last_sibling_after_every_sibling_rejected() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let first = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let second = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let rtt = Duration::from_millis(10);
    let a = register_backend(&app, "inst-a", &first, rtt).await;
    let b = register_backend(&app, "inst-b", &second, rtt).await;

    send_request(&app, "req-siblings-rejected")
        .await
        .expect("the last sibling admits the resend without an estimate");

    assert_resent_only_to_the_last_backend(&first, &second);

    app.state.end_registration(a).await;
    app.state.end_registration(b).await;
}

#[tokio::test]
async fn queue_mismatch_resends_to_the_last_cluster_after_every_cluster_rejected() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let first = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let second = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let rtt = Duration::from_millis(10);
    let a = register_backend_in(&app, "cluster-a", "inst-a", &first, rtt).await;
    let b = register_backend_in(&app, "cluster-b", "inst-b", &second, rtt).await;

    send_request(&app, "req-clusters-rejected")
        .await
        .expect("the last cluster admits the resend without an estimate");

    assert_resent_only_to_the_last_backend(&first, &second);

    app.state.end_registration(a).await;
    app.state.end_registration(b).await;
}

#[tokio::test]
async fn queue_mismatch_resend_without_an_estimate_happens_once() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let tunnel = MockTunnel::start(QUEUE_MISMATCH_REPLY);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    let status = send_request(&app, "req-always-rejected")
        .await
        .expect_err("a backend that keeps rejecting fails the request");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        tunnel.expected_queue_ms_headers(),
        [Some(BASE_QUEUE_MS), None]
    );

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn queue_mismatch_is_final_when_no_request_retries_are_allowed() {
    let mut app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    app.retry.max_request_retries = 0;
    let tunnel = MockTunnel::start(MockReply::MismatchWhenEstimated);
    let running = register_backend(&app, "inst-a", &tunnel, Duration::from_millis(10)).await;

    let status = send_request(&app, "req-no-retries")
        .await
        .expect_err("without request retries a mismatch is final");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(tunnel.expected_queue_ms_headers(), [Some(BASE_QUEUE_MS)]);

    app.state.end_registration(running).await;
}

#[tokio::test]
async fn queue_mismatch_tries_a_sibling_backend_with_its_estimate() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let rejecting = MockTunnel::start(QUEUE_MISMATCH_REPLY);
    let serving = MockTunnel::start(OK_REPLY);
    let rtt = Duration::from_millis(10);
    let a = register_backend(&app, "inst-a", &rejecting, rtt).await;
    let b = register_backend(&app, "inst-b", &serving, rtt).await;

    // Round robin reaches the rejecting backend within two requests.
    for request_id in ["req-sibling-1", "req-sibling-2"] {
        send_request(&app, request_id)
            .await
            .expect("the sibling backend serves the request");
    }

    assert!(rejecting.request_count() >= 1);
    assert_eq!(serving.request_count(), 2);
    for tunnel in [&rejecting, &serving] {
        assert!(
            tunnel
                .expected_queue_ms_headers()
                .iter()
                .all(Option::is_some),
            "no attempt is sent without an estimate while a sibling is left"
        );
    }

    app.state.end_registration(a).await;
    app.state.end_registration(b).await;
}

#[tokio::test]
async fn queue_mismatch_tries_another_cluster_with_its_estimate() {
    let app = proxy_app(UNEXPIRING_TTL, UNEXPIRING_TTL);
    let rejecting = MockTunnel::start(QUEUE_MISMATCH_REPLY);
    let serving = MockTunnel::start(OK_REPLY);
    let rtt = Duration::from_millis(10);
    let a = register_backend_in(&app, "cluster-a", "inst-a", &rejecting, rtt).await;
    let b = register_backend_in(&app, "cluster-b", "inst-b", &serving, rtt).await;

    for request_id in [
        "req-cluster-1",
        "req-cluster-2",
        "req-cluster-3",
        "req-cluster-4",
    ] {
        send_request(&app, request_id)
            .await
            .expect("the other cluster serves the request");
    }

    assert!(
        rejecting.request_count() >= 1,
        "the rejecting cluster must be tried for the test to cover the retry"
    );
    assert_eq!(serving.request_count(), 4);
    for tunnel in [&rejecting, &serving] {
        assert!(
            tunnel
                .expected_queue_ms_headers()
                .iter()
                .all(Option::is_some),
            "no attempt is sent without an estimate while another cluster is left"
        );
    }

    app.state.end_registration(a).await;
    app.state.end_registration(b).await;
}
