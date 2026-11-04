//! Shards: a LOCATION with a wildcard in its last path segment names many lance-nd tables.

use std::collections::HashMap;
use std::sync::Arc;

use beacon_plugin_sdk::datafusion::common::{DataFusionError, Result, plan_err};
use lance::io::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor};

/// `true` when `location` has a wildcard: `*` or `?`.
pub fn has_wildcard(location: &str) -> bool {
    location.contains(['*', '?'])
}

/// `true` when `name` matches `pattern`. `*` matches any run of characters, `?` one character.
pub fn matches(pattern: &str, name: &str) -> bool {
    let (pattern, name): (Vec<char>, Vec<char>) =
        (pattern.chars().collect(), name.chars().collect());
    let (mut p, mut n) = (0, 0);
    // The position after the last `*` and the name position it covers up to.
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p + 1, n));
                p += 1;
            }
            Some(&c) if c == '?' || c == name[n] => {
                p += 1;
                n += 1;
            }
            _ => match star {
                Some((after, covered)) => {
                    p = after;
                    n = covered + 1;
                    star = Some((after, covered + 1));
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|c| *c == '*')
}

/// The shards of `location` as `(name, uri)` pairs, sorted by name. The name is the last path
/// segment. A location without a wildcard is one shard, and this function does not list it.
pub async fn expand(
    location: &str,
    storage: &HashMap<String, String>,
) -> Result<Vec<(String, String)>> {
    let split = location.rfind(['/', '\\']);
    let (parent, pattern) = match split {
        Some(at) => (&location[..at], &location[at + 1..]),
        None => ("", location),
    };
    if has_wildcard(parent) {
        return plan_err!(
            "lance_nd: a wildcard is allowed in the last path segment only, not in `{location}`"
        );
    }
    if !has_wildcard(pattern) {
        return Ok(vec![(pattern.to_string(), location.to_string())]);
    }
    let separator = split.map(|at| &location[at..at + 1]);
    let parent = parent.trim_end_matches(['/', '\\']);
    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            storage.clone(),
        ))),
        ..Default::default()
    };
    let external = |e: lance::Error| DataFusionError::External(Box::new(e));
    let registry = Arc::new(ObjectStoreRegistry::default());
    let (store, path) = ObjectStore::from_uri_and_params(registry, parent, &params)
        .await
        .map_err(external)?;
    let mut names: Vec<String> = store
        .read_dir(path)
        .await
        .map_err(external)?
        .into_iter()
        .filter(|name| matches(pattern, name))
        .collect();
    names.sort();
    names.dedup();
    if names.is_empty() {
        return plan_err!("lance_nd: no lance-nd table matches `{location}`");
    }
    Ok(names
        .into_iter()
        .map(|name| {
            let uri = shard_uri(separator, parent, &name);
            (name, uri)
        })
        .collect())
}

/// The URI of the shard `name` in `parent`. `separator` is the separator of the LOCATION, or
/// `None` for a LOCATION without one, so a relative pattern gives a relative URI.
fn shard_uri(separator: Option<&str>, parent: &str, name: &str) -> String {
    match separator {
        Some(separator) => format!("{parent}{separator}{name}"),
        None => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::*;

    /// A temporary directory with the shard directories `b.lance` and `a.lance`, and a file.
    fn shard_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for name in ["b.lance", "a.lance"] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
            std::fs::write(dir.path().join(name).join("marker"), b"x").unwrap();
        }
        std::fs::write(dir.path().join("notes.txt"), b"x").unwrap();
        dir
    }

    fn names(found: &[(String, String)]) -> Vec<&str> {
        found.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn text(path: &Path) -> String {
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn wildcard_rules() {
        assert!(matches("*.lance", "aoml.lance"));
        assert!(matches("dac_?.lance", "dac_1.lance"));
        assert!(!matches("dac_?.lance", "dac_12.lance"));
        assert!(matches("*", ""));
        assert!(!matches("*.lance", "aoml.lance.bak"));
        assert!(has_wildcard("/a/*.lance") && !has_wildcard("/a/b.lance"));
    }

    #[tokio::test]
    async fn expand_lists_matching_entries() {
        let dir = shard_dir();
        let location = format!("{}/*.lance", text(dir.path()));
        let found = expand(&location, &HashMap::new()).await.unwrap();
        assert_eq!(names(&found), ["a.lance", "b.lance"]);
        assert!(found[0].1.ends_with("a.lance"), "{:?}", found[0]);
        assert!(Path::new(&found[0].1).is_dir(), "{:?}", found[0]);
    }

    #[tokio::test]
    async fn no_match_is_an_error() {
        let dir = shard_dir();
        let location = format!("{}/*.zarr", text(dir.path()));
        let error = expand(&location, &HashMap::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no lance-nd table matches"), "{error}");
    }

    #[tokio::test]
    async fn wildcard_before_the_last_segment_is_an_error() {
        let error = expand("/x/*/t.lance", &HashMap::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("last path segment"), "{error}");
    }

    #[tokio::test]
    async fn location_without_wildcard_is_one_shard() {
        let found = expand("/no/such/t.lance", &HashMap::new()).await.unwrap();
        assert_eq!(
            found,
            [("t.lance".to_string(), "/no/such/t.lance".to_string())]
        );
    }

    #[tokio::test]
    async fn windows_path_and_trailing_slash() {
        let dir = shard_dir();
        let joined = text(&dir.path().join("*.lance"));
        let doubled = format!("{}//*.lance", text(dir.path()));
        for location in [joined, doubled] {
            let found = expand(&location, &HashMap::new()).await.unwrap();
            assert_eq!(names(&found), ["a.lance", "b.lance"], "{location}");
            assert!(
                Path::new(&found[1].1).is_dir(),
                "{location}: {:?}",
                found[1]
            );
        }
    }

    #[test]
    fn shard_uri_keeps_the_location_form() {
        assert_eq!(shard_uri(None, "", "a.lance"), "a.lance");
        assert_eq!(shard_uri(Some("/"), "", "a.lance"), "/a.lance");
        assert_eq!(
            shard_uri(Some("/"), "s3://bucket/argo", "a.lance"),
            "s3://bucket/argo/a.lance"
        );
        assert_eq!(
            shard_uri(Some("\\"), "C:\\data", "a.lance"),
            "C:\\data\\a.lance"
        );
    }
}
