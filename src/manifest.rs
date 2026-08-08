// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

pub const MANIFEST_VERSION: &str = "1.0";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ComponentRef {
    pub name: String,
    pub version: String,
    pub artifact: ArtifactRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct LicenseRef {
    pub component: String,
    pub spdx: String,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct SignatureRef {
    pub scheme: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ComponentSet {
    pub connector: ComponentRef,
    pub signal_cli: ComponentRef,
    pub jre: ComponentRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct RuntimeManifest {
    pub manifest_version: String,
    pub bundle_id: String,
    pub created_at_unix_ms: u64,
    pub platform: String,
    pub components: ComponentSet,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
    pub licenses: Vec<LicenseRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sbom_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_archive: Option<ArtifactRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_record_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<SignatureRef>,
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("manifest io failed")]
    Io(#[from] io::Error),
    #[error("manifest is invalid")]
    Invalid,
    #[error("artifact hash mismatch for {0}")]
    HashMismatch(String),
    #[error("artifact is missing: {0}")]
    MissingArtifact(String),
    #[error("production signature is required but missing")]
    SignatureRequired,
    #[error("production signature is invalid")]
    InvalidSignature,
    #[error("production bundle is incomplete")]
    IncompleteProductionBundle,
    #[error("trusted key is invalid")]
    InvalidKey,
}

impl RuntimeManifest {
    pub fn load(path: &Path) -> Result<Self, ManifestError> {
        let text = fs::read_to_string(path)?;
        let manifest: Self = serde_json::from_str(&text).map_err(|_| ManifestError::Invalid)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn save(&self, path: &Path) -> Result<(), ManifestError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let text = serde_json::to_string_pretty(self).map_err(|_| ManifestError::Invalid)?;
        fs::write(path, text)?;
        Ok(())
    }

    pub fn verify_artifacts(&self, root: &Path) -> Result<(), ManifestError> {
        self.validate()?;
        if self.artifacts.is_empty() {
            for component in [
                &self.components.connector,
                &self.components.signal_cli,
                &self.components.jre,
            ] {
                verify_artifact(root, &component.artifact)?;
            }
            if let Some(source) = &self.source_archive {
                verify_artifact(root, source)?;
            }
            return Ok(());
        }

        let mut declared = self
            .artifacts
            .iter()
            .map(|artifact| artifact.path.as_str())
            .collect::<Vec<_>>();
        declared.sort_unstable();
        if declared.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ManifestError::Invalid);
        }
        for artifact in &self.artifacts {
            verify_artifact(root, artifact)?;
        }
        let discovered = list_bundle_files(root)?;
        if declared != discovered.iter().map(String::as_str).collect::<Vec<_>>() {
            return Err(ManifestError::Invalid);
        }
        for required in [
            &self.components.connector,
            &self.components.signal_cli,
            &self.components.jre,
        ] {
            if !declared.contains(&required.artifact.path.as_str()) {
                return Err(ManifestError::Invalid);
            }
        }
        if let Some(source) = &self.source_archive {
            if !declared.contains(&source.path.as_str()) {
                return Err(ManifestError::Invalid);
            }
        }
        Ok(())
    }

    pub fn sign_ed25519(
        &mut self,
        key_id: String,
        signing_key: &SigningKey,
    ) -> Result<(), ManifestError> {
        if !valid_key_id(&key_id) {
            return Err(ManifestError::InvalidKey);
        }
        self.signature = None;
        let signature = signing_key.sign(&self.signing_payload()?);
        self.signature = Some(SignatureRef {
            scheme: "ed25519".into(),
            key_id: Some(key_id),
            value: hex::encode(signature.to_bytes()),
        });
        Ok(())
    }

    pub fn verify_production(
        &self,
        root: &Path,
        trusted_key_id: &str,
        verifying_key: &VerifyingKey,
    ) -> Result<(), ManifestError> {
        self.verify_artifacts(root)?;
        if !matches!(
            self.platform.as_str(),
            "macos-arm64" | "macos-x64" | "windows-x64"
        ) || self.components.connector.name != "kt-signal-connector"
            || self.components.signal_cli.name != "signal-cli"
            || self.components.jre.name != "jre"
            || self.artifacts.is_empty()
            || self.source_archive.is_none()
            || self.sbom_path.is_none()
            || self.build_record_path.is_none()
        {
            return Err(ManifestError::IncompleteProductionBundle);
        }
        for (component, spdx) in [
            ("kt-signal-connector", Some("AGPL-3.0-only")),
            ("signal-cli", Some("GPL-3.0-only")),
            ("libsignal", Some("AGPL-3.0-only")),
            ("jre", None),
        ] {
            if !self.licenses.iter().any(|license| {
                license.component == component
                    && spdx.is_none_or(|expected| license.spdx == expected)
            }) {
                return Err(ManifestError::IncompleteProductionBundle);
            }
        }
        let declared = self
            .artifacts
            .iter()
            .map(|artifact| artifact.path.as_str())
            .collect::<Vec<_>>();
        if self
            .licenses
            .iter()
            .map(|license| license.path.as_str())
            .chain(self.sbom_path.iter().map(String::as_str))
            .chain(self.build_record_path.iter().map(String::as_str))
            .any(|path| !declared.contains(&path))
        {
            return Err(ManifestError::IncompleteProductionBundle);
        }
        let signature = self
            .signature
            .as_ref()
            .filter(|signature| {
                signature.scheme == "ed25519" && signature.key_id.as_deref() == Some(trusted_key_id)
            })
            .ok_or(ManifestError::SignatureRequired)?;
        let signature_bytes = decode_exact_hex::<64>(&signature.value)
            .map_err(|_| ManifestError::InvalidSignature)?;
        let signature = Signature::from_bytes(&signature_bytes);
        verifying_key
            .verify_strict(&self.signing_payload()?, &signature)
            .map_err(|_| ManifestError::InvalidSignature)
    }

    fn signing_payload(&self) -> Result<Vec<u8>, ManifestError> {
        let mut unsigned = self.clone();
        unsigned.signature = None;
        serde_json::to_vec(&unsigned).map_err(|_| ManifestError::Invalid)
    }

    fn validate(&self) -> Result<(), ManifestError> {
        if self.manifest_version != MANIFEST_VERSION
            || self.bundle_id.is_empty()
            || self.bundle_id.len() > 128
            || self.created_at_unix_ms > 9_007_199_254_740_991
            || self.licenses.is_empty()
        {
            return Err(ManifestError::Invalid);
        }
        for artifact in self
            .artifacts
            .iter()
            .chain([
                &self.components.connector.artifact,
                &self.components.signal_cli.artifact,
                &self.components.jre.artifact,
            ])
            .chain(self.source_archive.iter())
        {
            validate_artifact(artifact)?;
        }
        for path in self
            .licenses
            .iter()
            .map(|license| license.path.as_str())
            .chain(self.sbom_path.iter().map(String::as_str))
            .chain(self.build_record_path.iter().map(String::as_str))
        {
            validate_relative_path(path)?;
        }
        if self.licenses.iter().any(|license| {
            license.component.is_empty()
                || license.component.len() > 128
                || license.spdx.is_empty()
                || license.spdx.len() > 128
        }) {
            return Err(ManifestError::Invalid);
        }
        if let Some(signature) = &self.signature {
            if signature.scheme == "ed25519" {
                if !signature.key_id.as_deref().is_some_and(valid_key_id)
                    || decode_exact_hex::<64>(&signature.value).is_err()
                {
                    return Err(ManifestError::InvalidSignature);
                }
            } else if signature.scheme != "unsigned-local" || signature.key_id.is_some() {
                return Err(ManifestError::InvalidSignature);
            }
        }
        Ok(())
    }
}

pub fn load_signing_key(path: &Path) -> Result<SigningKey, ManifestError> {
    let mut bytes = Zeroizing::new(fs::read(path)?);
    if bytes.len() != 32 {
        return Err(ManifestError::InvalidKey);
    }
    let mut key = [0_u8; 32];
    key.copy_from_slice(&bytes);
    bytes.zeroize();
    Ok(SigningKey::from_bytes(&key))
}

pub fn load_verifying_key(path: &Path) -> Result<VerifyingKey, ManifestError> {
    let bytes = fs::read(path)?;
    let key: [u8; 32] = bytes.try_into().map_err(|_| ManifestError::InvalidKey)?;
    VerifyingKey::from_bytes(&key).map_err(|_| ManifestError::InvalidKey)
}

pub fn hash_file(path: &Path) -> Result<(String, u64), ManifestError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut size = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    Ok((hex::encode(hasher.finalize()), size))
}

pub fn artifact_for_file(root: &Path, relative: &str) -> Result<ArtifactRef, ManifestError> {
    validate_relative_path(relative)?;
    let path = root.join(relative);
    let (sha256, size_bytes) = hash_file(&path)?;
    Ok(ArtifactRef {
        path: relative.replace('\\', "/"),
        sha256,
        size_bytes,
    })
}

pub fn current_platform() -> &'static str {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        "macos-arm64"
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        "macos-x64"
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        "windows-x64"
    }
    #[cfg(not(any(
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "windows", target_arch = "x86_64")
    )))]
    {
        "unknown"
    }
}

fn verify_artifact(root: &Path, artifact: &ArtifactRef) -> Result<(), ManifestError> {
    validate_artifact(artifact)?;
    let path = root.join(&artifact.path);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|_| ManifestError::MissingArtifact(artifact.path.clone()))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ManifestError::MissingArtifact(artifact.path.clone()));
    }
    let (sha256, size_bytes) = hash_file(&path)?;
    if sha256 != artifact.sha256 || size_bytes != artifact.size_bytes {
        return Err(ManifestError::HashMismatch(artifact.path.clone()));
    }
    Ok(())
}

fn validate_artifact(artifact: &ArtifactRef) -> Result<(), ManifestError> {
    validate_relative_path(&artifact.path)?;
    if decode_exact_hex::<32>(&artifact.sha256).is_err() {
        return Err(ManifestError::Invalid);
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<(), ManifestError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ManifestError::Invalid);
    }
    Ok(())
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn decode_exact_hex<const N: usize>(value: &str) -> Result<[u8; N], ()> {
    if value.len() != N * 2
        || value
            .bytes()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(());
    }
    hex::decode(value)
        .map_err(|_| ())?
        .try_into()
        .map_err(|_| ())
}

fn list_bundle_files(root: &Path) -> Result<Vec<String>, ManifestError> {
    let mut files = Vec::new();
    collect_bundle_files(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_bundle_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<String>,
) -> Result<(), ManifestError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(ManifestError::Invalid);
        }
        if metadata.is_dir() {
            collect_bundle_files(root, &entry.path(), files)?;
        } else if metadata.is_file() {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| ManifestError::Invalid)?
                .to_string_lossy()
                .replace('\\', "/");
            if relative != "manifest.json" {
                validate_relative_path(&relative)?;
                files.push(relative);
            }
        } else {
            return Err(ManifestError::Invalid);
        }
    }
    Ok(())
}

fn artifacts_for_bundle(root: &Path) -> Result<Vec<ArtifactRef>, ManifestError> {
    list_bundle_files(root)?
        .iter()
        .map(|relative| artifact_for_file(root, relative))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn build_local_unsigned_manifest(
    root: &Path,
    bundle_id: String,
    connector_version: String,
    signal_cli_version: String,
    jre_version: String,
    connector_rel: &str,
    signal_cli_rel: &str,
    jre_rel: &str,
) -> Result<RuntimeManifest, ManifestError> {
    Ok(RuntimeManifest {
        manifest_version: MANIFEST_VERSION.into(),
        bundle_id,
        created_at_unix_ms: crate::link::now_ms(),
        platform: current_platform().into(),
        components: ComponentSet {
            connector: ComponentRef {
                name: "kt-signal-connector".into(),
                version: connector_version,
                artifact: artifact_for_file(root, connector_rel)?,
            },
            signal_cli: ComponentRef {
                name: "signal-cli".into(),
                version: signal_cli_version,
                artifact: artifact_for_file(root, signal_cli_rel)?,
            },
            jre: ComponentRef {
                name: "jre".into(),
                version: jre_version,
                artifact: artifact_for_file(root, jre_rel)?,
            },
        },
        artifacts: artifacts_for_bundle(root)?,
        licenses: vec![
            LicenseRef {
                component: "kt-signal-connector".into(),
                spdx: "AGPL-3.0-only".into(),
                path: "licenses/kt-signal-connector.AGPL-3.0-only.txt".into(),
            },
            LicenseRef {
                component: "signal-cli".into(),
                spdx: "GPL-3.0-only".into(),
                path: "licenses/NOTICE.txt".into(),
            },
        ],
        sbom_path: Some("sbom.cdx.json".into()),
        source_archive: None,
        build_record_path: Some("build-record.json".into()),
        signature: Some(SignatureRef {
            scheme: "unsigned-local".into(),
            key_id: None,
            value: "local-dev-not-for-production".into(),
        }),
    })
}

pub fn resolve_under_root(root: &Path, relative: &str) -> PathBuf {
    root.join(relative)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn hashes_and_verifies_local_manifest() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("connector.bin"), b"connector").unwrap();
        fs::write(temp.path().join("signal-cli.bin"), b"signal").unwrap();
        fs::write(temp.path().join("jre.bin"), b"jre").unwrap();
        let manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-1".into(),
            "0.1.0".into(),
            "0.14.7".into(),
            "25".into(),
            "connector.bin",
            "signal-cli.bin",
            "jre.bin",
        )
        .unwrap();
        manifest.save(&temp.path().join("manifest.json")).unwrap();
        let loaded = RuntimeManifest::load(&temp.path().join("manifest.json")).unwrap();
        loaded.verify_artifacts(temp.path()).unwrap();
        let wrong_key = SigningKey::from_bytes(&[9_u8; 32]);
        assert!(matches!(
            loaded.verify_production(temp.path(), "production-1", &wrong_key.verifying_key()),
            Err(ManifestError::IncompleteProductionBundle)
        ));
        fs::write(temp.path().join("connector.bin"), b"changed").unwrap();
        assert!(matches!(
            loaded.verify_artifacts(temp.path()),
            Err(ManifestError::HashMismatch(_))
        ));
    }

    #[test]
    fn production_signature_covers_manifest_and_complete_bundle() {
        let temp = TempDir::new().unwrap();
        fs::create_dir_all(temp.path().join("licenses")).unwrap();
        fs::write(temp.path().join("connector.bin"), b"connector").unwrap();
        fs::write(temp.path().join("signal-cli.bin"), b"signal").unwrap();
        fs::write(temp.path().join("jre.bin"), b"jre").unwrap();
        fs::write(
            temp.path()
                .join("licenses/kt-signal-connector.AGPL-3.0-only.txt"),
            b"AGPL",
        )
        .unwrap();
        fs::write(temp.path().join("licenses/NOTICE.txt"), b"NOTICE").unwrap();
        fs::write(temp.path().join("licenses/signal-cli.txt"), b"GPL").unwrap();
        fs::write(temp.path().join("licenses/libsignal.txt"), b"AGPL").unwrap();
        fs::write(temp.path().join("licenses/jre.txt"), b"JRE license").unwrap();
        fs::write(temp.path().join("sbom.cdx.json"), b"{}").unwrap();
        fs::write(temp.path().join("build-record.json"), b"{}").unwrap();
        fs::write(temp.path().join("source.tar.gz"), b"source").unwrap();
        let mut manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-production".into(),
            "0.1.0".into(),
            "0.14.7".into(),
            "25".into(),
            "connector.bin",
            "signal-cli.bin",
            "jre.bin",
        )
        .unwrap();
        manifest.platform = "macos-arm64".into();
        manifest.licenses = vec![
            LicenseRef {
                component: "kt-signal-connector".into(),
                spdx: "AGPL-3.0-only".into(),
                path: "licenses/kt-signal-connector.AGPL-3.0-only.txt".into(),
            },
            LicenseRef {
                component: "signal-cli".into(),
                spdx: "GPL-3.0-only".into(),
                path: "licenses/signal-cli.txt".into(),
            },
            LicenseRef {
                component: "libsignal".into(),
                spdx: "AGPL-3.0-only".into(),
                path: "licenses/libsignal.txt".into(),
            },
            LicenseRef {
                component: "jre".into(),
                spdx: "NOASSERTION".into(),
                path: "licenses/jre.txt".into(),
            },
        ];
        manifest.source_archive = Some(artifact_for_file(temp.path(), "source.tar.gz").unwrap());
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        manifest
            .sign_ed25519("production-1".into(), &signing_key)
            .unwrap();
        manifest
            .verify_production(temp.path(), "production-1", &signing_key.verifying_key())
            .unwrap();

        let mut missing_license = manifest.clone();
        missing_license
            .licenses
            .retain(|license| license.component != "libsignal");
        missing_license
            .sign_ed25519("production-1".into(), &signing_key)
            .unwrap();
        assert!(matches!(
            missing_license.verify_production(
                temp.path(),
                "production-1",
                &signing_key.verifying_key()
            ),
            Err(ManifestError::IncompleteProductionBundle)
        ));

        let mut tampered = manifest.clone();
        tampered.components.connector.version = "9.9.9".into();
        assert!(matches!(
            tampered.verify_production(temp.path(), "production-1", &signing_key.verifying_key()),
            Err(ManifestError::InvalidSignature)
        ));
        assert!(matches!(
            manifest.verify_production(
                temp.path(),
                "production-1",
                &SigningKey::from_bytes(&[8_u8; 32]).verifying_key()
            ),
            Err(ManifestError::InvalidSignature)
        ));
    }

    #[test]
    fn rejects_unknown_manifest_fields() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("connector.bin"), b"connector").unwrap();
        fs::write(temp.path().join("signal-cli.bin"), b"signal").unwrap();
        fs::write(temp.path().join("jre.bin"), b"jre").unwrap();
        let manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-1".into(),
            "0.1.0".into(),
            "0.14.7".into(),
            "25".into(),
            "connector.bin",
            "signal-cli.bin",
            "jre.bin",
        )
        .unwrap();
        let mut value = serde_json::to_value(manifest).unwrap();
        value["unexpected"] = serde_json::json!("must-not-be-accepted");
        fs::write(
            temp.path().join("manifest.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();

        assert!(matches!(
            RuntimeManifest::load(&temp.path().join("manifest.json")),
            Err(ManifestError::Invalid)
        ));
    }
}
