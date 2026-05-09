use std::collections::HashMap;
use std::sync::OnceLock;

#[allow(dead_code)]
fn load_cookies() -> HashMap<String, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("test_cookies.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    serde_json::from_str(&text).expect("invalid test_cookies.json")
}

#[allow(dead_code)]
fn cookies() -> &'static HashMap<String, String> {
    static COOKIES: OnceLock<HashMap<String, String>> = OnceLock::new();
    COOKIES.get_or_init(load_cookies)
}

#[allow(dead_code)]
pub fn bilibili_cookies() -> &'static str {
    cookies()
        .get("bilibili")
        .expect("missing 'bilibili' in test_cookies.json")
}

#[allow(dead_code)]
pub fn douyin_cookies() -> &'static str {
    cookies()
        .get("douyin")
        .expect("missing 'douyin' in test_cookies.json")
}
