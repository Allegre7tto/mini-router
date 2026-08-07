use prost_types::{ListValue, Struct, Value, value::Kind};
use router::epp::candidates::{CandidateError, parse};
use router::epp::response::{picked, served, unavailable};
use router::types::RoutingDecision;
use std::collections::BTreeMap;

fn metadata_subset(endpoints: &[&str]) -> Struct {
    let list = ListValue {
        values: endpoints
            .iter()
            .map(|e| Value {
                kind: Some(Kind::StringValue(e.to_string())),
            })
            .collect(),
    };
    let mut subset_fields = BTreeMap::new();
    subset_fields.insert(
        "x-gateway-destination-endpoint-subset".to_string(),
        Value {
            kind: Some(Kind::ListValue(list)),
        },
    );
    let subset_hint = Value {
        kind: Some(Kind::StructValue(Struct {
            fields: subset_fields,
        })),
    };
    let mut fields = BTreeMap::new();
    fields.insert("envoy.lb.subset_hint".to_string(), subset_hint);
    Struct { fields }
}

#[test]
fn parse_valid_candidates() {
    let subset = metadata_subset(&["10.0.0.2:8200", "10.0.0.3:8200", "10.0.0.2:8200"]);
    let parsed = parse(&subset, 8).unwrap();
    assert_eq!(parsed.len(), 2);
}

#[test]
fn parse_missing_namespace() {
    let s = Struct {
        fields: BTreeMap::new(),
    };
    let result = parse(&s, 8);
    assert!(matches!(result, Err(CandidateError::MissingSubset)));
}

#[test]
fn parse_empty_list() {
    let subset = metadata_subset(&[]);
    let result = parse(&subset, 8);
    assert!(result.is_err());
}

#[test]
fn parse_dns_name_rejected() {
    let subset = metadata_subset(&["example.com:8200", "10.0.0.2:8200"]);
    let result = parse(&subset, 8);
    assert!(result.is_err());
}

#[test]
fn parse_port_zero_rejected() {
    let subset = metadata_subset(&["10.0.0.2:0"]);
    let result = parse(&subset, 8);
    assert!(result.is_err());
}

#[test]
fn parse_exceeds_limit() {
    let subset = metadata_subset(&["10.0.0.1:8200", "10.0.0.2:8200", "10.0.0.3:8200"]);
    let result = parse(&subset, 2);
    assert!(matches!(result, Err(CandidateError::TooManyCandidates)));
}

#[test]
fn picked_sets_header_and_metadata() {
    let decision = RoutingDecision {
        decision_id: uuid::Uuid::new_v4(),
        endpoint: "10.0.0.2:8200".parse().unwrap(),
        generation: 1,
        matched_tokens: 4,
        effective_load: 1.0,
        body: b"mutated-body".to_vec(),
    };
    let resp = picked(&decision);
    assert!(resp.dynamic_metadata.is_some());
    let meta = resp.dynamic_metadata.as_ref().unwrap();
    let lb_field = meta.fields.get("envoy.lb").unwrap();
    let endpoint = lb_field
        .kind
        .as_ref()
        .and_then(|k| match k {
            Kind::StructValue(s) => s.fields.get("x-gateway-destination-endpoint"),
            _ => None,
        })
        .and_then(|v| match &v.kind {
            Some(Kind::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(endpoint, "10.0.0.2:8200");

    match &resp.response {
        Some(ep_resp) => match ep_resp {
            envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::RequestBody(br) => {
                let cr = br.response.as_ref().unwrap();
                let hm = cr.header_mutation.as_ref().unwrap();
                let hdr = &hm.set_headers[0].header.as_ref().unwrap();
                assert_eq!(hdr.key, "x-gateway-destination-endpoint");
                let body_mut = cr.body_mutation.as_ref().unwrap();
                match &body_mut.mutation {
                    Some(envoy_grpc_ext_proc::envoy::service::ext_proc::v3::body_mutation::Mutation::Body(b)) => {
                        assert_eq!(b.as_slice(), b"mutated-body");
                    }
                    _ => panic!("expected body mutation"),
                }
            }
            _ => panic!("expected RequestBody response"),
        },
        None => panic!("expected response"),
    }
}

#[test]
fn unavailable_returns_503() {
    let resp = unavailable("no candidates");
    match &resp.response {
        Some(ep_resp) => match ep_resp {
            envoy_grpc_ext_proc::envoy::service::ext_proc::v3::processing_response::Response::ImmediateResponse(ir) =>
            {
                let code = ir.status.as_ref().unwrap().code;
                assert_eq!(code, envoy_grpc_ext_proc::envoy::r#type::v3::StatusCode::ServiceUnavailable as i32);
            }
            _ => panic!("expected ImmediateResponse"),
        },
        None => panic!("expected response"),
    }
}

#[test]
fn served_extracts_endpoint() {
    let mut lb_fields = BTreeMap::new();
    lb_fields.insert(
        "x-gateway-destination-endpoint-served".to_string(),
        Value {
            kind: Some(Kind::StringValue("10.0.0.5:8200".to_string())),
        },
    );
    let lb_value = Value {
        kind: Some(Kind::StructValue(Struct { fields: lb_fields })),
    };
    let mut fields = BTreeMap::new();
    fields.insert("envoy.lb".to_string(), lb_value);
    let meta = Struct { fields };
    let result = served(&meta).unwrap();
    assert_eq!(result.unwrap().to_string(), "10.0.0.5:8200");
}

#[test]
fn served_missing_returns_none() {
    let meta = Struct {
        fields: BTreeMap::new(),
    };
    let result = served(&meta).unwrap();
    assert!(result.is_none());
}
