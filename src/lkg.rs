// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use ed25519_dalek::VerifyingKey;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::manifest::{ManifestError, RuntimeManifest};

#[derive(Debug, Error)]
pub enum LkgError {
    #[error("lkg io failed")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("runtime layout is invalid")]
    InvalidLayout,
    #[error("version is not staged: {0}")]
    NotStaged(String),
    #[error("runtime version already exists: {0}")]
    VersionExists(String),
    #[error("no active runtime is available")]
    NoActive,
    #[error("no last-known-good runtime is available")]
    NoLkg,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimePointer {
    pub version_id: String,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Debug)]
pub struct RuntimeLayout {
    root: PathBuf,
}

impl RuntimeLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn versions_dir(&self) -> PathBuf {
        self.root.join("versions")
    }

    pub fn version_dir(&self, version_id: &str) -> PathBuf {
        self.versions_dir().join(version_id)
    }

    pub fn ensure(&self) -> Result<(), LkgError> {
        fs::create_dir_all(self.versions_dir())?;
        Ok(())
    }

    pub fn stage_bundle(&self, version_id: &str, bundle_dir: &Path) -> Result<PathBuf, LkgError> {
        self.stage_bundle_with_policy(version_id, bundle_dir, None)
    }

    pub fn stage_production_bundle(
        &self,
        version_id: &str,
        bundle_dir: &Path,
        trusted_key_id: &str,
        verifying_key: &VerifyingKey,
    ) -> Result<PathBuf, LkgError> {
        self.stage_bundle_with_policy(
            version_id,
            bundle_dir,
            Some((trusted_key_id, verifying_key)),
        )
    }

    fn stage_bundle_with_policy(
        &self,
        version_id: &str,
        bundle_dir: &Path,
        trust: Option<(&str, &VerifyingKey)>,
    ) -> Result<PathBuf, LkgError> {
        validate_version_id(version_id)?;
        self.ensure()?;
        let target = self.version_dir(version_id);
        if target.exists() {
            return Err(LkgError::VersionExists(version_id.into()));
        }
        let staging = self
            .versions_dir()
            .join(format!(".staging-{version_id}-{}", random_suffix()));
        let staged = (|| {
            copy_dir_all(bundle_dir, &staging)?;
            verify_version(&staging, trust)?;
            fs::rename(&staging, &target)?;
            Ok::<(), LkgError>(())
        })();
        if staged.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        staged?;
        write_pointer(&self.root.join("staged.json"), version_id)?;
        Ok(target)
    }

    pub fn activate_staged(&self) -> Result<RuntimePointer, LkgError> {
        self.activate_staged_with_policy(None)
    }

    pub fn activate_staged_production(
        &self,
        trusted_key_id: &str,
        verifying_key: &VerifyingKey,
    ) -> Result<RuntimePointer, LkgError> {
        self.activate_staged_with_policy(Some((trusted_key_id, verifying_key)))
    }

    fn activate_staged_with_policy(
        &self,
        trust: Option<(&str, &VerifyingKey)>,
    ) -> Result<RuntimePointer, LkgError> {
        let staged = match read_pointer(&self.root.join("staged.json"))? {
            Some(staged) => staged,
            None => {
                // Crash replay: activation already completed and staged.json was removed.
                let active = read_pointer(&self.root.join("active.json"))?
                    .ok_or_else(|| LkgError::NotStaged("staged".into()))?;
                verify_version(&self.version_dir(&active.version_id), trust)
                    .map_err(|_| LkgError::NotStaged("staged".into()))?;
                return Ok(active);
            }
        };
        let version_dir = self.version_dir(&staged.version_id);
        verify_version(&version_dir, trust)
            .map_err(|_| LkgError::NotStaged(staged.version_id.clone()))?;

        if let Some(active) = read_pointer(&self.root.join("active.json"))? {
            // Crash replay: active already points at the staged version, so the current
            // LKG is still the real last-known-good and must not be overwritten.
            if active.version_id != staged.version_id {
                let active_dir = self.version_dir(&active.version_id);
                if verify_version(&active_dir, trust).is_ok() {
                    write_pointer(&self.root.join("lkg.json"), &active.version_id)?;
                } else {
                    eprintln!(
                        "kt-signal-connector: previous active runtime failed verification; keeping existing LKG"
                    );
                }
            }
        }
        write_pointer(&self.root.join("active.json"), &staged.version_id)?;
        let _ = fs::remove_file(self.root.join("staged.json"));
        read_pointer(&self.root.join("active.json"))?.ok_or(LkgError::NoActive)
    }

    pub fn rollback_to_lkg(&self) -> Result<RuntimePointer, LkgError> {
        self.rollback_to_lkg_with_policy(None)
    }

    pub fn rollback_to_lkg_production(
        &self,
        trusted_key_id: &str,
        verifying_key: &VerifyingKey,
    ) -> Result<RuntimePointer, LkgError> {
        self.rollback_to_lkg_with_policy(Some((trusted_key_id, verifying_key)))
    }

    fn rollback_to_lkg_with_policy(
        &self,
        trust: Option<(&str, &VerifyingKey)>,
    ) -> Result<RuntimePointer, LkgError> {
        let lkg = read_pointer(&self.root.join("lkg.json"))?.ok_or(LkgError::NoLkg)?;
        let version_dir = self.version_dir(&lkg.version_id);
        verify_version(&version_dir, trust).map_err(|_| LkgError::NoLkg)?;
        write_pointer(&self.root.join("active.json"), &lkg.version_id)?;
        read_pointer(&self.root.join("active.json"))?.ok_or(LkgError::NoActive)
    }

    pub fn active(&self) -> Result<Option<RuntimePointer>, LkgError> {
        read_pointer(&self.root.join("active.json"))
    }

    pub fn lkg(&self) -> Result<Option<RuntimePointer>, LkgError> {
        read_pointer(&self.root.join("lkg.json"))
    }

    pub fn staged(&self) -> Result<Option<RuntimePointer>, LkgError> {
        read_pointer(&self.root.join("staged.json"))
    }
}

fn write_pointer(path: &Path, version_id: &str) -> Result<(), LkgError> {
    validate_version_id(version_id)?;
    let pointer = RuntimePointer {
        version_id: version_id.to_string(),
        updated_at_unix_ms: crate::link::now_ms(),
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("tmp-{}", random_suffix()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(
        serde_json::to_string_pretty(&pointer)
            .map_err(|_| LkgError::InvalidLayout)?
            .as_bytes(),
    )?;
    file.sync_all()?;
    drop(file);
    if let Err(error) = replace_file(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    sync_parent_dir(path)?;
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_dir(path: &Path) -> Result<(), LkgError> {
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(windows)]
fn sync_parent_dir(_path: &Path) -> Result<(), LkgError> {
    Ok(())
}

const MAX_POINTER_BYTES: u64 = 64 * 1024;

fn read_pointer(path: &Path) -> Result<Option<RuntimePointer>, LkgError> {
    if !path.exists() {
        return Ok(None);
    }
    if fs::metadata(path)?.len() > MAX_POINTER_BYTES {
        return Err(LkgError::InvalidLayout);
    }
    let text = fs::read_to_string(path)?;
    let pointer: RuntimePointer =
        serde_json::from_str(&text).map_err(|_| LkgError::InvalidLayout)?;
    validate_version_id(&pointer.version_id)?;
    Ok(Some(pointer))
}

fn verify_version(
    version_dir: &Path,
    trust: Option<(&str, &VerifyingKey)>,
) -> Result<(), LkgError> {
    if !version_dir.join("manifest.json").is_file() {
        return Err(LkgError::InvalidLayout);
    }
    let manifest = RuntimeManifest::load(&version_dir.join("manifest.json"))?;
    if let Some((key_id, key)) = trust {
        manifest.verify_production(version_dir, key_id, key)?;
    } else {
        manifest.verify_artifacts(version_dir)?;
    }
    Ok(())
}

fn validate_version_id(version_id: &str) -> Result<(), LkgError> {
    if version_id.is_empty()
        || version_id.len() > 128
        || !version_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        || version_id == "."
        || version_id == ".."
    {
        return Err(LkgError::InvalidLayout);
    }
    Ok(())
}

fn random_suffix() -> String {
    let mut bytes = [0_u8; 8];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(not(windows))]
fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<(), LkgError> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        } else {
            return Err(LkgError::InvalidLayout);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::build_local_unsigned_manifest;
    use tempfile::TempDir;

    fn write_bundle(dir: &Path) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("connector.bin"), b"connector-v1").unwrap();
        fs::write(dir.join("signal-cli.bin"), b"signal-v1").unwrap();
        fs::write(dir.join("jre.bin"), b"jre-v1").unwrap();
        let manifest = build_local_unsigned_manifest(
            dir,
            "bundle-1".into(),
            "0.1.0".into(),
            "0.14.7".into(),
            "25".into(),
            "connector.bin",
            "signal-cli.bin",
            "jre.bin",
        )
        .unwrap();
        manifest.save(&dir.join("manifest.json")).unwrap();
    }

    #[test]
    fn stage_activate_and_rollback() {
        let temp = TempDir::new().unwrap();
        let layout = RuntimeLayout::new(temp.path().join("runtime"));
        let bundle_a = temp.path().join("bundle-a");
        let bundle_b = temp.path().join("bundle-b");
        write_bundle(&bundle_a);
        write_bundle(&bundle_b);
        fs::write(bundle_b.join("connector.bin"), b"connector-v2").unwrap();
        let manifest = build_local_unsigned_manifest(
            &bundle_b,
            "bundle-2".into(),
            "0.1.1".into(),
            "0.14.7".into(),
            "25".into(),
            "connector.bin",
            "signal-cli.bin",
            "jre.bin",
        )
        .unwrap();
        manifest.save(&bundle_b.join("manifest.json")).unwrap();

        layout.stage_bundle("v1", &bundle_a).unwrap();
        let active = layout.activate_staged().unwrap();
        assert_eq!(active.version_id, "v1");
        assert!(layout.lkg().unwrap().is_none());

        layout.stage_bundle("v2", &bundle_b).unwrap();
        let active = layout.activate_staged().unwrap();
        assert_eq!(active.version_id, "v2");
        assert_eq!(layout.lkg().unwrap().unwrap().version_id, "v1");

        let rolled = layout.rollback_to_lkg().unwrap();
        assert_eq!(rolled.version_id, "v1");
        assert_eq!(layout.active().unwrap().unwrap().version_id, "v1");
    }

    #[test]
    fn repeated_activate_of_the_same_version_is_idempotent() {
        let temp = TempDir::new().unwrap();
        let layout = RuntimeLayout::new(temp.path().join("runtime"));
        let bundle = temp.path().join("bundle");
        write_bundle(&bundle);

        layout.stage_bundle("v1", &bundle).unwrap();
        let first = layout.activate_staged().unwrap();
        assert_eq!(first.version_id, "v1");
        // staged.json is gone; a repeated activate completes idempotently.
        let second = layout.activate_staged().unwrap();
        assert_eq!(second.version_id, "v1");
        assert!(layout.lkg().unwrap().is_none());
    }

    #[test]
    fn replayed_activate_keeps_the_real_lkg() {
        let temp = TempDir::new().unwrap();
        let layout = RuntimeLayout::new(temp.path().join("runtime"));
        let bundle_a = temp.path().join("bundle-a");
        let bundle_b = temp.path().join("bundle-b");
        write_bundle(&bundle_a);
        write_bundle(&bundle_b);
        fs::write(bundle_b.join("connector.bin"), b"connector-v2").unwrap();
        let manifest = build_local_unsigned_manifest(
            &bundle_b,
            "bundle-2".into(),
            "0.1.1".into(),
            "0.14.7".into(),
            "25".into(),
            "connector.bin",
            "signal-cli.bin",
            "jre.bin",
        )
        .unwrap();
        manifest.save(&bundle_b.join("manifest.json")).unwrap();

        layout.stage_bundle("v1", &bundle_a).unwrap();
        layout.activate_staged().unwrap();
        layout.stage_bundle("v2", &bundle_b).unwrap();
        layout.activate_staged().unwrap();
        assert_eq!(layout.lkg().unwrap().unwrap().version_id, "v1");

        // Simulate a crash after active.json was written but before staged.json was removed.
        write_pointer(&layout.root().join("staged.json"), "v2").unwrap();
        let replayed = layout.activate_staged().unwrap();
        assert_eq!(replayed.version_id, "v2");
        assert_eq!(layout.lkg().unwrap().unwrap().version_id, "v1");
        assert!(layout.staged().unwrap().is_none());
    }

    #[test]
    fn oversized_pointer_is_rejected() {
        let temp = TempDir::new().unwrap();
        let layout = RuntimeLayout::new(temp.path().join("runtime"));
        layout.ensure().unwrap();
        fs::write(
            layout.root().join("active.json"),
            " ".repeat((MAX_POINTER_BYTES + 1) as usize),
        )
        .unwrap();
        assert!(matches!(layout.active(), Err(LkgError::InvalidLayout)));
    }

    #[test]
    fn staging_is_immutable_and_rejects_corrupt_pointer_paths() {
        let temp = TempDir::new().unwrap();
        let layout = RuntimeLayout::new(temp.path().join("runtime"));
        let bundle = temp.path().join("bundle");
        write_bundle(&bundle);

        let version = layout.stage_bundle("v1", &bundle).unwrap();
        assert!(matches!(
            layout.stage_bundle("v1", &bundle),
            Err(LkgError::VersionExists(version)) if version == "v1"
        ));
        assert_eq!(
            fs::read(version.join("connector.bin")).unwrap(),
            b"connector-v1"
        );

        fs::write(
            layout.root().join("active.json"),
            r#"{"versionId":"../outside","updatedAtUnixMs":1}"#,
        )
        .unwrap();
        assert!(matches!(layout.active(), Err(LkgError::InvalidLayout)));
    }

    #[test]
    fn failed_stage_never_publishes_a_partial_version() {
        let temp = TempDir::new().unwrap();
        let layout = RuntimeLayout::new(temp.path().join("runtime"));
        let bundle = temp.path().join("bad-bundle");
        write_bundle(&bundle);
        fs::write(bundle.join("connector.bin"), b"tampered").unwrap();

        assert!(layout.stage_bundle("bad", &bundle).is_err());
        assert!(!layout.version_dir("bad").exists());
        assert!(layout.staged().unwrap().is_none());
        let leftovers = fs::read_dir(layout.versions_dir())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty());
    }
}
