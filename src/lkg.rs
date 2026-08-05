// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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
        if version_id.is_empty()
            || version_id.contains('/')
            || version_id.contains('\\')
            || version_id.contains("..")
        {
            return Err(LkgError::InvalidLayout);
        }
        self.ensure()?;
        let target = self.version_dir(version_id);
        if target.exists() {
            fs::remove_dir_all(&target)?;
        }
        copy_dir_all(bundle_dir, &target)?;
        let manifest = RuntimeManifest::load(&target.join("manifest.json"))?;
        manifest.verify_artifacts(&target, false)?;
        write_pointer(&self.root.join("staged.json"), version_id)?;
        Ok(target)
    }

    pub fn activate_staged(&self) -> Result<RuntimePointer, LkgError> {
        let staged = read_pointer(&self.root.join("staged.json"))?
            .ok_or_else(|| LkgError::NotStaged("staged".into()))?;
        let version_dir = self.version_dir(&staged.version_id);
        if !version_dir.join("manifest.json").is_file() {
            return Err(LkgError::NotStaged(staged.version_id));
        }
        let manifest = RuntimeManifest::load(&version_dir.join("manifest.json"))?;
        manifest.verify_artifacts(&version_dir, false)?;

        if let Some(active) = read_pointer(&self.root.join("active.json"))? {
            write_pointer(&self.root.join("lkg.json"), &active.version_id)?;
        }
        write_pointer(&self.root.join("active.json"), &staged.version_id)?;
        let _ = fs::remove_file(self.root.join("staged.json"));
        read_pointer(&self.root.join("active.json"))?.ok_or(LkgError::NoActive)
    }

    pub fn rollback_to_lkg(&self) -> Result<RuntimePointer, LkgError> {
        let lkg = read_pointer(&self.root.join("lkg.json"))?.ok_or(LkgError::NoLkg)?;
        let version_dir = self.version_dir(&lkg.version_id);
        if !version_dir.join("manifest.json").is_file() {
            return Err(LkgError::NoLkg);
        }
        let manifest = RuntimeManifest::load(&version_dir.join("manifest.json"))?;
        manifest.verify_artifacts(&version_dir, false)?;
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
    let pointer = RuntimePointer {
        version_id: version_id.to_string(),
        updated_at_unix_ms: crate::link::now_ms(),
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(&pointer).unwrap())?;
    Ok(())
}

fn read_pointer(path: &Path) -> Result<Option<RuntimePointer>, LkgError> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    let pointer = serde_json::from_str(&text).map_err(|_| LkgError::InvalidLayout)?;
    Ok(Some(pointer))
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
}
