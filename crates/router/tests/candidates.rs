use extproc::envoy::service::ext_proc::v3::{
    body_mutation::Mutation, processing_response::Response,
};
use prost_types::{ListValue, Struct, Value, value::Kind};
use router::epp::candidates::{CandidateError, parse};
use router::epp::candidates::{SUBSET_NAMESPACE, optional};
use router::epp::response::{picked, served, unavailable};
use router::types::RoutingDecision;
use std::collections::{BTreeMap, HashMap};

fn metadata_subset(endpoints: &[&str]) -> HashMap<String, Struct> {
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
    HashMap::from([(
        "envoy.lb.subset_hint".to_string(),
        Struct {
            fields: subset_fields,
        },
    )])
}

#[test]
fn parse_valid_candidates() {
    let subset = metadata_subset(&["10.0.0.2:8200", "10.0.0.3:8200", "10.0.0.2:8200"]);
    let parsed = parse(&subset, 8).unwrap();
    assert_eq!(parsed.len(), 2);
}

#[test]
fn parse_missing_namespace() {
    let result = parse(&HashMap::new(), 8);
    assert!(matches!(result, Err(CandidateError::MissingSubset)));
}

#[test]
fn absent_subset_is_optional_but_invalid_subset_is_rejected() {
    assert!(optional(&HashMap::new(), 8).unwrap().is_none());
    let empty_namespace = HashMap::from([(SUBSET_NAMESPACE.into(), Struct::default())]);
    assert!(optional(&empty_namespace, 8).unwrap().is_none());
    assert!(optional(&metadata_subset(&[]), 8).is_err());
    assert!(optional(&metadata_subset(&["bad"]), 8).is_err());
    assert_eq!(
        optional(&metadata_subset(&["10.0.0.1:8200"]), 8)
            .unwrap()
            .unwrap()
            .len(),
        1
    );
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
    let resp = picked(&decision, "token");
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
            Response::RequestHeaders(br) => {
                let cr = br.response.as_ref().unwrap();
                let hm = cr.header_mutation.as_ref().unwrap();
                assert!(
                    hm.set_headers
                        .iter()
                        .all(|header| header.append_action == 2)
                );
                let hdr = &hm.set_headers[0].header.as_ref().unwrap();
                assert_eq!(hdr.key, "x-gateway-destination-endpoint");
                assert_eq!(hdr.raw_value, b"10.0.0.2:8200");
                assert_eq!(hm.remove_headers, ["content-length"]);
                assert_eq!(hm.set_headers.len(), 2);
                assert_eq!(
                    hm.set_headers[1].header.as_ref().unwrap().key,
                    "x-mini-router-token"
                );
                assert!(cr.body_mutation.is_none());
            }
            _ => panic!("expected RequestHeaders response"),
        },
        None => panic!("expected response"),
    }
}

#[test]
fn unavailable_returns_503() {
    let resp = unavailable("no candidates");
    match &resp.response {
        Some(ep_resp) => match ep_resp {
            Response::ImmediateResponse(ir) => {
                let code = ir.status.as_ref().unwrap().code;
                assert_eq!(code, 503);
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
    let meta = HashMap::from([("envoy.lb".to_string(), Struct { fields: lb_fields })]);
    let result = served(&meta).unwrap();
    assert_eq!(result.unwrap().to_string(), "10.0.0.5:8200");
}

#[test]
fn served_missing_returns_none() {
    let result = served(&HashMap::new()).unwrap();
    assert!(result.is_none());
}

#[test]
fn full_duplex_chunks_preserve_bytes_and_end_flags() {
    for size in [0, 1, 62_000, 62_001, 150_000] {
        let original = vec![42; size];
        for request in [false, true] {
            for end in [false, true] {
                let responses: Vec<_> =
                    router::epp::response::bodies(&original, end, request).collect();
                let mut restored = Vec::new();
                for (index, response) in responses.iter().enumerate() {
                    let body = match response.response.as_ref().unwrap() {
                        Response::RequestBody(body) if request => body,
                        Response::ResponseBody(body) if !request => body,
                        _ => panic!("unexpected body direction"),
                    };
                    let Some(Mutation::StreamedResponse(chunk)) = &body
                        .response
                        .as_ref()
                        .unwrap()
                        .body_mutation
                        .as_ref()
                        .unwrap()
                        .mutation
                    else {
                        panic!("expected streamed response")
                    };
                    assert!(chunk.body.len() <= 62_000);
                    assert_eq!(chunk.end_of_stream, end && index + 1 == responses.len());
                    restored.extend_from_slice(&chunk.body);
                }
                assert_eq!(restored, original);
            }
        }
    }
}
