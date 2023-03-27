use bytes::Bytes;

#[allow(dead_code)]
pub fn get_test_key(i: usize) -> Bytes {
    Bytes::from(std::format!("bitcast-rs-test-key-{:09}", i))
}

#[allow(dead_code)]
pub fn get_test_value(i: usize) -> Bytes {
    Bytes::from(std::format!(
        "bitcast-rs-test-value-value-value-value-value-value-value-value-value-value-value-{:09}",
        i
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_test_key_value() {
        let (key, value) = (get_test_key(100), get_test_value(100));
        assert!(key.len() > 10);
        assert!(value.len() > 20);
    }
}
