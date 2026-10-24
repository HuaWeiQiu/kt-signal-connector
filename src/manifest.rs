// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

pub const MANIFEST_VERSION: &str = "1.0";

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

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
    /// Legacy signal-cli component (ADR 0003 removal): new bundles carry no
    /// signal-cli, but an LKG rollback can still point at a manifest written
    /// before the removal — the field deserializes for that read, keeps its
    /// production gates armed, and round-trips on re-serialize; new manifests
    /// never emit the key (absent components are skipped, not nulled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_cli: Option<ComponentRef>,
    /// Legacy JRE component: same rollback tolerance as `signal_cli`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jre: Option<ComponentRef>,
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
    #[error("manifest exceeds the 1 MiB size limit")]
    TooLarge,
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
        if fs::metadata(path)?.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestError::TooLarge);
        }
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
            for component in self.component_refs() {
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
        for required in self.component_refs() {
            if !declared.contains(&required.artifact.path.as_str()) {
                return Err(ManifestError::Invalid);
            }
        }
        if let Some(source) = &self.source_archive
            && !declared.contains(&source.path.as_str())
        {
            return Err(ManifestError::Invalid);
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
            || self
                .components
                .signal_cli
                .as_ref()
                .is_some_and(|component| component.name != "signal-cli")
            || self
                .components
                .jre
                .as_ref()
                .is_some_and(|component| component.name != "jre")
            || self.artifacts.is_empty()
            || self.source_archive.is_none()
            || self.sbom_path.is_none()
            || self.build_record_path.is_none()
        {
            return Err(ManifestError::IncompleteProductionBundle);
        }
        let mut required_licenses = vec![
            ("kt-signal-connector", Some("AGPL-3.0-only")),
            ("libsignal", Some("AGPL-3.0-only")),
        ];
        if self.components.signal_cli.is_some() {
            required_licenses.push(("signal-cli", Some("GPL-3.0-only")));
        }
        if self.components.jre.is_some() {
            required_licenses.push(("jre", None));
        }
        for (component, spdx) in required_licenses {
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

    /// The connector plus whichever legacy components this manifest still
    /// declares (an LKG rollback can point at a pre-removal bundle).
    fn component_refs(&self) -> impl Iterator<Item = &ComponentRef> {
        std::iter::once(&self.components.connector)
            .chain(self.components.signal_cli.iter())
            .chain(self.components.jre.iter())
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
            .chain(self.component_refs().map(|component| &component.artifact))
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
    let mut key = Zeroizing::new([0_u8; 32]);
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

pub fn build_local_unsigned_manifest(
    root: &Path,
    bundle_id: String,
    connector_version: String,
    connector_rel: &str,
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
            signal_cli: None,
            jre: None,
        },
        artifacts: artifacts_for_bundle(root)?,
        licenses: vec![LicenseRef {
            component: "kt-signal-connector".into(),
            spdx: "AGPL-3.0-only".into(),
            path: "licenses/kt-signal-connector.AGPL-3.0-only.txt".into(),
        }],
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn hashes_and_verifies_local_manifest() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("connector.bin"), b"connector").unwrap();
        let manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-1".into(),
            "0.1.0".into(),
            "connector.bin",
        )
        .unwrap();
        assert!(manifest.components.signal_cli.is_none());
        assert!(manifest.components.jre.is_none());
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

    /// LKG rollback tolerance (ADR 0003): a manifest written before the
    /// signal-cli/JRE removal still declares `signalCli`/`jre` components.
    /// Those fields deserialize, keep their component gates armed, and the
    /// manifest verifies against a bundle that still contains the files.
    #[test]
    fn legacy_manifest_with_signal_cli_and_jre_still_loads_and_verifies() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("connector.bin"), b"connector").unwrap();
        fs::write(temp.path().join("signal-cli.bin"), b"signal").unwrap();
        fs::write(temp.path().join("jre.bin"), b"jre").unwrap();
        let manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-legacy".into(),
            "0.1.0".into(),
            "connector.bin",
        )
        .unwrap();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value["components"]["signalCli"] = serde_json::json!({
            "name": "signal-cli",
            "version": "0.14.7",
            "artifact": artifact_for_file(temp.path(), "signal-cli.bin").unwrap(),
        });
        value["components"]["jre"] = serde_json::json!({
            "name": "jre",
            "version": "25",
            "artifact": artifact_for_file(temp.path(), "jre.bin").unwrap(),
        });
        fs::write(
            temp.path().join("manifest.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();

        let loaded = RuntimeManifest::load(&temp.path().join("manifest.json")).unwrap();
        assert_eq!(
            loaded
                .components
                .signal_cli
                .as_ref()
                .expect("legacy component must deserialize")
                .version,
            "0.14.7"
        );
        assert_eq!(
            loaded
                .components
                .jre
                .as_ref()
                .expect("legacy component must deserialize")
                .version,
            "25"
        );
        loaded.verify_artifacts(temp.path()).unwrap();
        // The legacy components round-trip: a re-saved legacy manifest keeps
        // declaring them, while a fresh manifest (no components) never gains
        // the keys (absent is skipped, not nulled).
        let serialized = serde_json::to_value(&loaded).unwrap();
        assert_eq!(serialized["components"]["signalCli"]["version"], "0.14.7");
        assert_eq!(serialized["components"]["jre"]["version"], "25");
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
        fs::write(temp.path().join("licenses/libsignal.txt"), b"AGPL").unwrap();
        // The legacy rollback leg re-declares the signal-cli/JRE components,
        // whose required license entries point at these files.
        fs::write(temp.path().join("licenses/signal-cli.txt"), b"GPL").unwrap();
        fs::write(temp.path().join("licenses/jre.txt"), b"jre").unwrap();
        fs::write(temp.path().join("sbom.cdx.json"), b"{}").unwrap();
        fs::write(temp.path().join("build-record.json"), b"{}").unwrap();
        fs::write(temp.path().join("source.tar.gz"), b"source").unwrap();
        let mut manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-production".into(),
            "0.1.0".into(),
            "connector.bin",
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
                component: "libsignal".into(),
                spdx: "AGPL-3.0-only".into(),
                path: "licenses/libsignal.txt".into(),
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

        // Legacy rollback leg: re-declaring signal-cli/JRE components on the
        // same bundle restores their production gates — name checks and the
        // signal-cli GPL / JRE license requirements.
        let mut legacy = manifest.clone();
        legacy.components.signal_cli = Some(ComponentRef {
            name: "signal-cli".into(),
            version: "0.14.7".into(),
            artifact: artifact_for_file(temp.path(), "signal-cli.bin").unwrap(),
        });
        legacy.components.jre = Some(ComponentRef {
            name: "jre".into(),
            version: "25".into(),
            artifact: artifact_for_file(temp.path(), "jre.bin").unwrap(),
        });
        legacy.licenses.push(LicenseRef {
            component: "signal-cli".into(),
            spdx: "GPL-3.0-only".into(),
            path: "licenses/signal-cli.txt".into(),
        });
        legacy.licenses.push(LicenseRef {
            component: "jre".into(),
            spdx: "NOASSERTION".into(),
            path: "licenses/jre.txt".into(),
        });
        legacy
            .sign_ed25519("production-1".into(), &signing_key)
            .unwrap();
        legacy
            .verify_production(temp.path(), "production-1", &signing_key.verifying_key())
            .unwrap();

        let mut wrong_name = legacy.clone();
        wrong_name.components.signal_cli.as_mut().unwrap().name = "not-signal-cli".into();
        wrong_name
            .sign_ed25519("production-1".into(), &signing_key)
            .unwrap();
        assert!(matches!(
            wrong_name.verify_production(temp.path(), "production-1", &signing_key.verifying_key()),
            Err(ManifestError::IncompleteProductionBundle)
        ));

        let mut missing_license = legacy.clone();
        missing_license
            .licenses
            .retain(|license| license.component != "signal-cli");
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

        let mut missing_jre_license = legacy.clone();
        missing_jre_license
            .licenses
            .retain(|license| license.component != "jre");
        missing_jre_license
            .sign_ed25519("production-1".into(), &signing_key)
            .unwrap();
        assert!(matches!(
            missing_jre_license.verify_production(
                temp.path(),
                "production-1",
                &signing_key.verifying_key()
            ),
            Err(ManifestError::IncompleteProductionBundle)
        ));

        let mut missing_libsignal = manifest.clone();
        missing_libsignal
            .licenses
            .retain(|license| license.component != "libsignal");
        missing_libsignal
            .sign_ed25519("production-1".into(), &signing_key)
            .unwrap();
        assert!(matches!(
            missing_libsignal.verify_production(
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
    fn rejects_manifest_over_size_limit() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("manifest.json");
        fs::write(&path, " ".repeat((MAX_MANIFEST_BYTES + 1) as usize)).unwrap();
        assert!(matches!(
            RuntimeManifest::load(&path),
            Err(ManifestError::TooLarge)
        ));
    }

    #[test]
    fn rejects_unknown_manifest_fields() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join("connector.bin"), b"connector").unwrap();
        let manifest = build_local_unsigned_manifest(
            temp.path(),
            "bundle-1".into(),
            "0.1.0".into(),
            "connector.bin",
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
