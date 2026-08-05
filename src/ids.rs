// SPDX-License-Identifier: AGPL-3.0-only

use sha2::{Digest, Sha256};

pub fn random_id() -> String {
    use rand::RngCore;
    let mut value = [0_u8; 16];
    rand::rng().fill_bytes(&mut value);
    hex::encode(value)
}

pub fn stable_hash_id(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            hasher.update([0]);
        }
        hasher.update(part.as_bytes());
    }
    hex::encode(hasher.finalize())
}

pub fn mask_address(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    if chars.len() <= 6 {
        return "*".repeat(chars.len().max(1));
    }
    let head: String = chars.iter().take(3).collect();
    let tail: String = chars.iter().rev().take(2).rev().collect();
    format!("{head}***{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_hides_middle_without_logging_full_number() {
        let masked = mask_address("+15555550100");
        assert!(masked.starts_with("+15"));
        assert!(masked.ends_with("00"));
        assert!(masked.contains("***"));
        assert!(!masked.contains("555555"));
    }

    #[test]
    fn stable_ids_are_deterministic() {
        assert_eq!(stable_hash_id(&["a", "b"]), stable_hash_id(&["a", "b"]));
        assert_ne!(stable_hash_id(&["a", "b"]), stable_hash_id(&["a", "c"]));
    }
}
