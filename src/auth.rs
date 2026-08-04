// SPDX-License-Identifier: AGPL-3.0-only

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::API_VERSION;

const SECRET_BYTES: usize = 32;
const HEX_SECRET_BYTES: usize = SECRET_BYTES * 2;
const PROOF_CONTEXT: &[u8] = b"kt-signal-connector-v1\0";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("bootstrap secret file is not private")]
    InsecurePermissions,
    #[error("bootstrap secret must be exactly 64 lowercase hexadecimal characters")]
    InvalidSecret,
    #[error("authentication proof is invalid")]
    InvalidProof,
    #[error("authentication input is invalid")]
    InvalidInput,
    #[error("failed to read bootstrap secret")]
    Read(#[source] io::Error),
    #[error("failed to remove bootstrap secret")]
    Remove(#[source] io::Error),
}

#[derive(Zeroize)]
#[zeroize(drop)]
pub struct BootstrapSecret([u8; SECRET_BYTES]);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Challenge {
    pub server_nonce: String,
    pub algorithm: &'static str,
    pub api_version: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandshakeParams {
    pub client_nonce: String,
    pub proof: String,
}

pub struct PendingChallenge {
    nonce: [u8; SECRET_BYTES],
}

impl PendingChallenge {
    pub fn generate() -> Self {
        let mut nonce = [0_u8; SECRET_BYTES];
        rand::rng().fill_bytes(&mut nonce);
        Self { nonce }
    }

    pub fn public(&self) -> Challenge {
        Challenge {
            server_nonce: hex::encode(self.nonce),
            algorithm: "HMAC-SHA256",
            api_version: API_VERSION,
        }
    }

    pub fn verify(
        &self,
        secret: &BootstrapSecret,
        params: &HandshakeParams,
    ) -> Result<(), AuthError> {
        let client_nonce = decode_canonical_hex::<SECRET_BYTES>(&params.client_nonce)?;
        let proof = decode_canonical_hex::<SECRET_BYTES>(&params.proof)?;
        let mut mac = HmacSha256::new_from_slice(&secret.0).expect("HMAC accepts a 32-byte key");
        mac.update(PROOF_CONTEXT);
        mac.update(hex::encode(self.nonce).as_bytes());
        mac.update(b"\0");
        mac.update(hex::encode(client_nonce).as_bytes());
        mac.update(b"\0");
        mac.update(API_VERSION.as_bytes());
        mac.verify_slice(&proof)
            .map_err(|_| AuthError::InvalidProof)
    }
}

pub fn load_bootstrap_secret(path: &Path) -> Result<BootstrapSecret, AuthError> {
    if !has_private_parent(path) {
        return Err(AuthError::InsecurePermissions);
    }
    let metadata = fs::symlink_metadata(path).map_err(AuthError::Read)?;
    if !metadata.file_type().is_file() || !has_private_permissions(&metadata) {
        return Err(AuthError::InsecurePermissions);
    }

    let file = File::open(path).map_err(AuthError::Read)?;
    let opened_metadata = file.metadata().map_err(AuthError::Read)?;
    if !same_file(&metadata, &opened_metadata) {
        return Err(AuthError::InsecurePermissions);
    }
    let read_result = read_limited(file);
    let remove_result = fs::remove_file(path);
    let mut encoded = read_result?;
    remove_result.map_err(AuthError::Remove)?;

    if encoded.len() != HEX_SECRET_BYTES
        || encoded
            .iter()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(AuthError::InvalidSecret);
    }
    let text = std::str::from_utf8(&encoded).map_err(|_| AuthError::InvalidSecret)?;
    let bytes = Zeroizing::new(hex::decode(text).map_err(|_| AuthError::InvalidSecret)?);
    encoded.zeroize();
    let mut secret = [0_u8; SECRET_BYTES];
    secret.copy_from_slice(&bytes);
    Ok(BootstrapSecret(secret))
}

fn read_limited(file: File) -> Result<Zeroizing<Vec<u8>>, AuthError> {
    let mut encoded = Zeroizing::new(Vec::with_capacity(HEX_SECRET_BYTES + 1));
    file.take((HEX_SECRET_BYTES + 2) as u64)
        .read_to_end(&mut encoded)
        .map_err(AuthError::Read)?;
    Ok(encoded)
}

#[cfg(unix)]
fn has_private_parent(path: &Path) -> bool {
    path.parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .and_then(|parent| fs::metadata(parent).ok())
        .is_some_and(|metadata| metadata.is_dir() && has_private_permissions(&metadata))
}

#[cfg(windows)]
fn has_private_parent(_path: &Path) -> bool {
    true
}

#[cfg(unix)]
fn same_file(first: &fs::Metadata, second: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    first.dev() == second.dev() && first.ino() == second.ino()
}

#[cfg(windows)]
fn same_file(_first: &fs::Metadata, _second: &fs::Metadata) -> bool {
    true
}

fn decode_canonical_hex<const N: usize>(encoded: &str) -> Result<[u8; N], AuthError> {
    if encoded.len() != N * 2
        || encoded
            .bytes()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(AuthError::InvalidInput);
    }
    hex::decode(encoded)
        .map_err(|_| AuthError::InvalidInput)?
        .try_into()
        .map_err(|_| AuthError::InvalidInput)
}

#[cfg(unix)]
fn has_private_permissions(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    metadata.permissions().mode() & 0o077 == 0
        && metadata.uid() == rustix::process::getuid().as_raw()
}

#[cfg(windows)]
fn has_private_permissions(_metadata: &fs::Metadata) -> bool {
    // The Windows launcher creates this file with a current-user-only DACL. Phase 3 adds an
    // explicit ACL verifier before Windows packaging is accepted.
    true
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    impl BootstrapSecret {
        pub(crate) fn for_test(bytes: [u8; SECRET_BYTES]) -> Self {
            Self(bytes)
        }
    }

    #[test]
    fn proof_verifies_and_rejects_changes() {
        let secret = BootstrapSecret([7; SECRET_BYTES]);
        let challenge = PendingChallenge {
            nonce: [9; SECRET_BYTES],
        };
        let client_nonce = [11; SECRET_BYTES];
        let mut mac = HmacSha256::new_from_slice(&secret.0).unwrap();
        mac.update(PROOF_CONTEXT);
        mac.update(hex::encode(challenge.nonce).as_bytes());
        mac.update(b"\0");
        mac.update(hex::encode(client_nonce).as_bytes());
        mac.update(b"\0");
        mac.update(API_VERSION.as_bytes());
        let params = HandshakeParams {
            client_nonce: hex::encode(client_nonce),
            proof: hex::encode(mac.finalize().into_bytes()),
        };
        assert!(challenge.verify(&secret, &params).is_ok());

        let mut changed = params;
        let replacement = if &changed.proof[0..1] == "0" {
            "1"
        } else {
            "0"
        };
        changed.proof.replace_range(0..1, replacement);
        assert!(challenge.verify(&secret, &changed).is_err());
    }

    #[test]
    #[cfg(unix)]
    fn secret_file_is_private_exact_and_removed_after_read() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::TempDir::new().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let mut file = NamedTempFile::new_in(temp.path()).unwrap();
        file.as_file_mut()
            .write_all(&[b'a'; HEX_SECRET_BYTES])
            .unwrap();
        file.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .unwrap();
        let path = file.path().to_path_buf();
        file.keep().unwrap();

        assert!(load_bootstrap_secret(&path).is_ok());
        assert!(!path.exists());
    }

    #[test]
    #[cfg(unix)]
    fn secret_file_rejects_extra_newline_and_is_still_removed() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::TempDir::new().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let mut file = NamedTempFile::new_in(temp.path()).unwrap();
        file.as_file_mut()
            .write_all(&[b'a'; HEX_SECRET_BYTES])
            .unwrap();
        file.as_file_mut().write_all(b"\n").unwrap();
        file.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .unwrap();
        let path = file.path().to_path_buf();
        file.keep().unwrap();

        assert!(matches!(
            load_bootstrap_secret(&path),
            Err(AuthError::InvalidSecret)
        ));
        assert!(!path.exists());
    }
}
