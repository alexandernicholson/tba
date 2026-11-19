//! Offline evaluation of `TimeBasedAutoscaler` manifests (`tba evaluate`).

use std::io::Write;

use jiff::Timestamp;
use jiff::tz::TimeZone;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Deserialize;

use crate::crd::TimeBasedAutoscalerSpec;
use crate::error::Error;
use crate::schedule::CompiledSchedule;

/// A YAML document: a `TimeBasedAutoscaler`, a `List` of them, or anything
/// else (skipped). Decoded in one pass without an intermediate value.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Document {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    metadata: ObjectMeta,
    spec: Option<TimeBasedAutoscalerSpec>,
    #[serde(default)]
    items: Vec<Self>,
}

/// Evaluates every `TimeBasedAutoscaler` in `input` at `now` and writes one
/// tab-separated line per object. Returns the number of objects evaluated.
///
/// `input` is a YAML stream or, when it starts with `{`, a stream of JSON
/// objects such as `kubectl get -o json` output. JSON decodes far faster.
///
/// # Errors
///
/// Fails on undecodable input, invalid schedules or write errors.
pub fn evaluate(input: &str, now: Timestamp, tz: &TimeZone, out: &mut impl Write) -> Result<usize, Error> {
    let decode = |error: &dyn std::fmt::Display| Error::Decode(error.to_string());
    let mut pending: Vec<Document> = if input.trim_start().starts_with('{') {
        serde_json::Deserializer::from_str(input).into_iter().collect::<Result<_, _>>().map_err(|e| decode(&e))?
    } else {
        let options = serde_saphyr::options! { budget: None };
        let documents: Vec<Option<Document>> =
            serde_saphyr::from_multiple_with_options(input, options).map_err(|e| decode(&e))?;
        documents.into_iter().flatten().collect()
    };
    writeln!(out, "NAMESPACE\tNAME\tFLOOR\tACTIVE\tNEXT")?;
    let mut count = 0;
    pending.reverse();
    while let Some(document) = pending.pop() {
        match document.kind.as_str() {
            "List" => pending.extend(document.items.into_iter().rev()),
            "TimeBasedAutoscaler" => {
                write_line(&document, now, tz, out)?;
                count += 1;
            }
            _ => {}
        }
    }
    out.flush()?;
    Ok(count)
}

fn write_line(document: &Document, now: Timestamp, tz: &TimeZone, out: &mut impl Write) -> Result<(), Error> {
    let namespace = document.metadata.namespace.as_deref().unwrap_or("default");
    let name = document.metadata.name.as_deref().unwrap_or_default();
    let spec = document.spec.as_ref().ok_or_else(|| Error::Decode(format!("{namespace}/{name}: missing spec")))?;
    let compiled =
        CompiledSchedule::compile(spec).map_err(|error| Error::Decode(format!("{namespace}/{name}: {error}")))?;
    let evaluation = compiled.evaluate(now, tz);
    let active = compiled.active_at(evaluation.minute);
    let active = if active.is_empty() { "-".to_owned() } else { active.join(",") };
    write!(out, "{namespace}\t{name}\t{}\t{active}\t", evaluation.floor)?;
    match evaluation.next_change {
        Some(next) => writeln!(out, "{}", next.to_zoned(tz.clone()))?,
        None => writeln!(out, "-")?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFESTS: &str = r#"
apiVersion: tba.alexandernicholson.github.io/v1alpha1
kind: TimeBasedAutoscaler
metadata:
  name: web
  namespace: shop
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 2
  schedules:
  - name: business
    days: [Monday, Tuesday, Wednesday, Thursday, Friday]
    start: "08:00"
    end: "18:00"
    minReplicas: 10
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ignored
---
---
apiVersion: v1
kind: List
items:
- apiVersion: tba.alexandernicholson.github.io/v1alpha1
  kind: TimeBasedAutoscaler
  metadata:
    name: batch
  spec:
    scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: batch}
    minReplicas: 1
"#;

    fn run(input: &str) -> Result<(usize, String), Error> {
        let mut out = Vec::new();
        let berlin = TimeZone::get("Europe/Berlin").unwrap();
        let count = evaluate(input, "2026-01-05T12:00:00Z".parse().unwrap(), &berlin, &mut out)?;
        Ok((count, String::from_utf8(out).unwrap()))
    }

    #[test]
    fn evaluates_documents_and_lists() {
        let (count, output) = run(MANIFESTS).unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            output,
            "NAMESPACE\tNAME\tFLOOR\tACTIVE\tNEXT\n\
             shop\tweb\t10\tbusiness\t2026-01-05T18:00:00+01:00[Europe/Berlin]\n\
             default\tbatch\t1\t-\t-\n"
        );
    }

    #[test]
    fn evaluates_kubectl_json_output() {
        let list = r#"{"apiVersion":"v1","kind":"List","items":[{"kind":"TimeBasedAutoscaler","metadata":{"name":"web","namespace":"shop"},
            "spec":{"scaleTargetRef":{"apiVersion":"apps/v1","kind":"Deployment","name":"web"},"minReplicas":3}}]}
            {"kind":"TimeBasedAutoscaler","metadata":{"name":"api"},"spec":{"scaleTargetRef":{"apiVersion":"apps/v1","kind":"Deployment","name":"api"},"minReplicas":1}}"#;
        let (count, output) = run(list).unwrap();
        assert_eq!(count, 2);
        assert!(output.ends_with("shop\tweb\t3\t-\t-\ndefault\tapi\t1\t-\t-\n"), "{output}");
        assert!(run("{\"kind\": 1}").unwrap_err().to_string().starts_with("decoding input failed:"));
    }

    #[test]
    fn reports_bad_input() {
        let error = |input: &str| run(input).unwrap_err().to_string();
        assert!(error("kind: [").starts_with("decoding input failed:"));
        assert!(error("{").starts_with("decoding input failed:"));
        assert_eq!(
            error("kind: TimeBasedAutoscaler\nmetadata: {name: a}"),
            "decoding input failed: default/a: missing spec"
        );
        let invalid = MANIFESTS.replace("\"18:00\"", "\"18:00:00\"");
        assert_eq!(
            error(&invalid),
            r#"decoding input failed: shop/web: schedule "business": invalid time "18:00:00", expected HH:MM"#
        );
    }
}
