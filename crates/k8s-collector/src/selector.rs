use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::core::Selector;

use crate::CollectorError;

/// Validate before using kube's renderer: its conversion alone does not check
/// label syntax or reject values on Exists/DoesNotExist. All requirements are
/// ANDed; an empty Deployment selector is rejected instead of listing all pods.
pub(crate) fn pod_selector(selector: &LabelSelector) -> Result<String, CollectorError> {
    let labels = selector.match_labels.as_ref();
    let expressions = selector.match_expressions.as_deref().unwrap_or_default();
    if labels.is_none_or(|labels| labels.is_empty()) && expressions.is_empty() {
        return Err(CollectorError::InvalidSelector);
    }
    for (key, value) in labels.into_iter().flatten() {
        if !valid_key(key) || !valid_value(value) {
            return Err(CollectorError::InvalidSelector);
        }
    }
    for expression in expressions {
        if !valid_key(&expression.key) {
            return Err(CollectorError::InvalidSelector);
        }
        let values = expression.values.as_deref().unwrap_or_default();
        match expression.operator.as_str() {
            "In" | "NotIn" if !values.is_empty() && values.iter().all(|v| valid_value(v)) => {}
            "Exists" | "DoesNotExist" if values.is_empty() => {}
            _ => return Err(CollectorError::InvalidSelector),
        }
    }
    let converted: Selector = selector
        .clone()
        .try_into()
        .map_err(|_| CollectorError::InvalidSelector)?;
    Ok(converted.to_string())
}

fn valid_key(key: &str) -> bool {
    match key.split_once('/') {
        Some((prefix, name)) => {
            !prefix.is_empty()
                && prefix.len() <= 253
                && prefix.split('.').all(|part| {
                    bounded_token(part, |b| b.is_ascii_lowercase() || b.is_ascii_digit(), b"-")
                })
                && valid_name(name)
        }
        None => valid_name(key),
    }
}

fn valid_name(name: &str) -> bool {
    name.len() <= 63 && bounded_token(name, |b| b.is_ascii_alphanumeric(), b"-_.")
}

fn valid_value(value: &str) -> bool {
    value.is_empty() || valid_name(value)
}

fn bounded_token(value: &str, edge: impl Fn(u8) -> bool, interior: &[u8]) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && edge(bytes[0])
        && edge(bytes[bytes.len() - 1])
        && bytes.iter().all(|&b| edge(b) || interior.contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    #[test]
    fn renders_all_standard_operators_and_match_labels() {
        let selector = LabelSelector {
            match_labels: Some([("app.example.io/name".into(), "finance".into())].into()),
            match_expressions: Some(vec![
                requirement("env", "In", Some(vec!["prod", "staging"])),
                requirement("tier", "NotIn", Some(vec!["batch"])),
                requirement("enabled", "Exists", None),
                requirement("disabled", "DoesNotExist", Some(vec![])),
            ]),
        };
        assert_eq!(
            pod_selector(&selector).unwrap(),
            "app.example.io/name=finance,env in (prod,staging),tier notin (batch),enabled,!disabled"
        );
    }

    fn requirement(
        key: &str,
        operator: &str,
        values: Option<Vec<&str>>,
    ) -> LabelSelectorRequirement {
        LabelSelectorRequirement {
            key: key.into(),
            operator: operator.into(),
            values: values.map(|v| v.into_iter().map(String::from).collect()),
        }
    }

    #[test]
    fn refuses_invalid_operators_values_keys_and_empty_selectors() {
        assert_eq!(
            pod_selector(&LabelSelector::default()),
            Err(CollectorError::InvalidSelector)
        );
        for req in [
            requirement("app", "GreaterThan", Some(vec!["1"])),
            requirement("app", "In", None),
            requirement("app", "NotIn", Some(vec![])),
            requirement("app", "Exists", Some(vec!["ignored"])),
            requirement("app", "DoesNotExist", Some(vec!["ignored"])),
            requirement("app,other", "In", Some(vec!["finance"])),
            requirement("app", "In", Some(vec!["finance,other"])),
            requirement("Uppercase.example/app", "Exists", None),
            requirement("app/extra/slash", "Exists", None),
        ] {
            let selector = LabelSelector {
                match_expressions: Some(vec![req]),
                ..Default::default()
            };
            assert_eq!(
                pod_selector(&selector),
                Err(CollectorError::InvalidSelector)
            );
        }
    }

    #[test]
    fn accepts_empty_label_values_and_valid_qualified_keys() {
        let selector = LabelSelector {
            match_labels: Some([("example.io/Name_1".into(), "".into())].into()),
            match_expressions: Some(vec![requirement("empty", "In", Some(vec![""]))]),
        };
        assert_eq!(
            pod_selector(&selector).unwrap(),
            "example.io/Name_1=,empty in ()"
        );
    }

    #[test]
    fn enforces_label_length_boundaries() {
        assert!(valid_name(&"a".repeat(63)));
        assert!(!valid_name(&"a".repeat(64)));
        assert!(!valid_value(" value "));
        assert!(!valid_key("-bad/name"));
        assert!(!valid_key("example..io/name"));
        assert!(!valid_key(&format!("{}/name", "a".repeat(254))));
    }
}
