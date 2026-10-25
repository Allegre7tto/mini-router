#![cfg(feature = "mock")]

use std::time::Duration;

use tokio::process::Command;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn readiness() {
    ready(false).await;
}

#[tokio::test]
async fn tls_epp_with_plaintext_health() {
    ready(true).await;
}

async fn ready(tls: bool) {
    // Mock renderer processes also bind fixed KV event ports.
    static PROCESSES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = PROCESSES.lock().await;
    let api = wiremock::MockServer::start().await;
    for (path, body) in [
        (
            "/apis/inference.networking.k8s.io/v1/namespaces/mini-router/inferencepools",
            serde_json::json!({
                "apiVersion": "inference.networking.k8s.io/v1", "kind": "InferencePoolList",
                "metadata": {"resourceVersion": "1"},
                "items": [{"apiVersion": "inference.networking.k8s.io/v1", "kind": "InferencePool", "metadata": {"name": "router"},
                    "spec": {"selector": {"matchLabels": {"app": "worker"}}, "targetPorts": [{"number": 8200}]}}]
            }),
        ),
        (
            "/api/v1/namespaces/mini-router/pods",
            serde_json::json!({
                "apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "1"}, "items": []
            }),
        ),
    ] {
        wiremock::Mock::given(wiremock::matchers::path(path))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(body))
            .mount(&api)
            .await;
    }
    wiremock::Mock::given(wiremock::matchers::query_param("watch", "true"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .with_priority(1)
        .mount(&api)
        .await;
    let kubeconfig = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        kubeconfig.path(),
        format!(
            r#"
apiVersion: v1
kind: Config
clusters:
  - name: test
    cluster:
      server: {}
contexts:
  - name: test
    context:
      cluster: test
      user: test
current-context: test
users:
  - name: test
    user: {{}}
"#,
            api.uri()
        ),
    )
    .unwrap();
    let render = port();
    let grpc = port();
    let metrics = port();
    let health = port();
    let certs = tempfile::tempdir().unwrap();
    let signed = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = signed.cert.pem();
    std::fs::write(certs.path().join("cert.pem"), &pem).unwrap();
    std::fs::write(
        certs.path().join("key.pem"),
        signed.signing_key.serialize_pem(),
    )
    .unwrap();
    let mut mock = Command::new(env!("CARGO_BIN_EXE_mock"))
        .env("MOCK_LISTEN", format!("127.0.0.1:{render}"))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_router"));
    command
        .env_remove("CERT")
        .env_remove("KEY")
        .env("KUBECONFIG", kubeconfig.path())
        .env("NAMESPACE", "mini-router")
        .env("POOL", "router")
        .env("WORKLOAD_TOKEN", "test-token")
        .env("MODEL", "fixed")
        .env("REVISION", "abc123")
        .env(
            "PROBE_DIGEST",
            router::config::RouterConfig::fixture().probe_digest,
        )
        .env("RENDERER_URL", format!("http://127.0.0.1:{render}"))
        .env("EPP_LISTEN", format!("127.0.0.1:{grpc}"))
        .env("METRICS_LISTEN", format!("127.0.0.1:{metrics}"))
        .env("HEALTH", format!("127.0.0.1:{health}"))
        .kill_on_drop(true);
    if tls {
        command
            .env("CERT", certs.path().join("cert.pem"))
            .env("KEY", certs.path().join("key.pem"));
    }
    let mut router = command.spawn().unwrap();

    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(router.try_wait().unwrap().is_none(), "router exited");
            assert!(mock.try_wait().unwrap().is_none(), "renderer exited");
            if let Ok(channel) =
                tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{health}"))
                    .unwrap()
                    .connect()
                    .await
            {
                let mut client = HealthClient::new(channel);
                let response = client
                    .check(HealthCheckRequest {
                        service: "envoy.service.ext_proc.v3.ExternalProcessor".into(),
                    })
                    .await
                    .unwrap();
                if response.into_inner().status == ServingStatus::Serving as i32 {
                    let response = client
                        .check(HealthCheckRequest {
                            service: String::new(),
                        })
                        .await
                        .unwrap();
                    assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("router did not become ready");

    let endpoint = tonic::transport::Endpoint::from_shared(format!(
        "{}://127.0.0.1:{grpc}",
        if tls { "https" } else { "http" }
    ))
    .unwrap();
    let endpoint = if tls {
        endpoint
            .tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(tonic::transport::Certificate::from_pem(pem)),
            )
            .unwrap()
    } else {
        endpoint
    };
    let mut client = HealthClient::new(endpoint.connect().await.unwrap());
    assert_eq!(
        client
            .check(HealthCheckRequest {
                service: "envoy.service.ext_proc.v3.ExternalProcessor".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .status,
        ServingStatus::Serving as i32
    );

    for stream in [false, true] {
        let response = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{render}/v1/chat/completions"))
            .json(&serde_json::json!({"stream": stream}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        assert!(response.headers().contains_key("x-mock-worker"));
        if stream {
            assert_eq!(response.headers()["content-type"], "text/event-stream");
        }
        let body = response.text().await.unwrap();
        assert!(body.contains("mock response"));
        assert_eq!(body.contains("data: [DONE]"), stream);
    }

    router.kill().await.unwrap();
    mock.kill().await.unwrap();
}
