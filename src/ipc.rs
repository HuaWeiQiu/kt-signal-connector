// SPDX-License-Identifier: AGPL-3.0-only

use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
mod platform {
    use std::fs;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

    use tokio::net::{UnixListener, UnixStream};

    use super::*;

    pub type LocalStream = UnixStream;

    pub struct LocalListener {
        inner: UnixListener,
        path: PathBuf,
        device: u64,
        inode: u64,
    }

    impl LocalListener {
        pub fn bind(endpoint: &Path) -> io::Result<Self> {
            let path = prepare_endpoint(endpoint)?;
            let inner = UnixListener::bind(&path)?;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
            let metadata = fs::symlink_metadata(&path)?;
            Ok(Self {
                inner,
                path,
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }

        pub async fn accept(&self) -> io::Result<LocalStream> {
            self.inner.accept().await.map(|(stream, _)| stream)
        }
    }

    impl Drop for LocalListener {
        fn drop(&mut self) {
            let should_remove = fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
                metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode
            });
            if should_remove {
                let _ = fs::remove_file(&self.path);
            }
        }
    }

    fn prepare_endpoint(endpoint: &Path) -> io::Result<PathBuf> {
        let file_name = endpoint.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "endpoint must include a socket file name",
            )
        })?;
        let parent = endpoint.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "endpoint must include a parent directory",
            )
        })?;
        if !parent.exists() {
            fs::create_dir_all(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let metadata = fs::symlink_metadata(parent)?;
        if !metadata.is_dir()
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != rustix::process::getuid().as_raw()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "endpoint directory must be private",
            ));
        }
        let canonical_parent = fs::canonicalize(parent)?;
        let path = canonical_parent.join(file_name);
        if fs::symlink_metadata(&path).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "endpoint already exists",
            ));
        }
        Ok(path)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn socket_is_private_and_removed_with_listener() {
            let temp = tempfile::TempDir::new().unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let endpoint = temp.path().join("connector.sock");
            let listener = LocalListener::bind(&endpoint).unwrap();
            assert_eq!(
                fs::metadata(&endpoint).unwrap().permissions().mode() & 0o777,
                0o600
            );
            drop(listener);
            assert!(!endpoint.exists());
        }

        #[test]
        fn insecure_endpoint_directory_is_rejected() {
            let temp = tempfile::TempDir::new().unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755)).unwrap();
            let endpoint = temp.path().join("connector.sock");
            match LocalListener::bind(&endpoint) {
                Ok(_) => panic!("insecure endpoint directory must be rejected"),
                Err(error) => assert_eq!(error.kind(), io::ErrorKind::PermissionDenied),
            }
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;

    pub struct LocalListener;
    pub type LocalStream = tokio::net::windows::named_pipe::NamedPipeServer;

    impl LocalListener {
        pub fn bind(_endpoint: &Path) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Windows named-pipe transport is pending platform acceptance",
            ))
        }

        pub async fn accept(&self) -> io::Result<LocalStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Windows transport is unavailable",
            ))
        }
    }
}

pub use platform::{LocalListener, LocalStream};
