use tonic_health::server::HealthReporter;

pub fn reporter() -> HealthReporter {
    let (reporter, _service) = tonic_health::server::health_reporter();
    reporter
}
