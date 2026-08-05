// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const MANIFEST_VERSION: &str = "1.0";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactRef {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ComponentRef {
    pub name: String,
    pub version: String,
    pub artifact: ArtifactRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LicenseRef {
    pub component: String,
    pub spdx: String,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SignatureRef {
    pub scheme: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ComponentSet {
    pub connector: ComponentRef,
    pub signal_cli: ComponentRef,
    pub jre: ComponentRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeManifest {
    pub manifest_version: String,
    pub bundle_id: String,
    pub created_at_unix_ms: u64,
    pub platform: String,
    pub components: ComponentSet,
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
}

impl RuntimeManifest {
    pub fn load(path: &Path) -> Result<Self, ManifestError> {
        let text = fs::read_to_string(path)?;
        let manifest: Self = serde_json::from_str(&text).map_err(|_| ManifestError::Invalid)?;
        if manifest.manifest_version != MANIFEST_VERSION {
            return Err(ManifestError::Invalid);
        }
        if manifest.bundle_id.is_empty() || manifest.licenses.is_empty() {
            return Err(ManifestError::Invalid);
        }
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

    pub fn verify_artifacts(
        &self,
        root: &Path,
        require_signature: bool,
    ) -> Result<(), ManifestError> {
        if require_signature {
            match self.signature.as_ref() {
                Some(signature)
                    if signature.scheme != "unsigned-local" && !signature.value.is_empty() => {}
                _ => return Err(ManifestError::SignatureRequired),
            }
        }
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
        Ok(())
    }
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
    let path = root.join(&artifact.path);
    if !path.is_file() {
        return Err(ManifestError::MissingArtifact(artifact.path.clone()));
    }
    let (sha256, size_bytes) = hash_file(&path)?;
    if sha256 != artifact.sha256 || size_bytes != artifact.size_bytes {
        return Err(ManifestError::HashMismatch(artifact.path.clone()));
    }
    Ok(())
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
        licenses: vec![
            LicenseRef {
                component: "kt-signal-connector".into(),
                spdx: "AGPL-3.0-only".into(),
                path: "LICENSE".into(),
            },
            LicenseRef {
                component: "signal-cli".into(),
                spdx: "GPL-3.0-only".into(),
                path: "NOTICE".into(),
            },
        ],
        sbom_path: Some("sbom.cdx.json".into()),
        source_archive: None,
        build_record_path: Some("build-record.json".into()),
        signature: Some(SignatureRef {
            scheme: "unsigned-local".into(),
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
        loaded.verify_artifacts(temp.path(), false).unwrap();
        assert!(loaded.verify_artifacts(temp.path(), true).is_err());
        fs::write(temp.path().join("connector.bin"), b"changed").unwrap();
        assert!(matches!(
            loaded.verify_artifacts(temp.path(), false),
            Err(ManifestError::HashMismatch(_))
        ));
    }
}
