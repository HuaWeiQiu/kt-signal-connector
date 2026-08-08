// SPDX-License-Identifier: AGPL-3.0-only

use std::io;
use std::path::Path;

#[cfg(unix)]
use std::path::PathBuf;

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
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr;
    use std::sync::Mutex;

    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetTokenInformation, PROTECTED_DACL_SECURITY_INFORMATION,
        PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, SetFileSecurityW, TOKEN_QUERY, TOKEN_USER,
        TokenUser,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    use super::*;

    pub type LocalStream = NamedPipeServer;

    pub struct LocalListener {
        name: String,
        server: Mutex<NamedPipeServer>,
    }

    impl LocalListener {
        pub fn bind(endpoint: &Path) -> io::Result<Self> {
            let name = normalize_pipe_name(endpoint)?;
            let server = create_server(&name, true)?;
            Ok(Self {
                name,
                server: Mutex::new(server),
            })
        }

        pub async fn accept(&self) -> io::Result<LocalStream> {
            // Take the current server, wait for a client, then immediately create the next
            // listening instance so the host can accept again after disconnect.
            let server = {
                let mut slot = self
                    .server
                    .lock()
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "pipe listener poisoned"))?;
                let next = create_server(&self.name, false)?;
                std::mem::replace(&mut *slot, next)
            };
            server.connect().await?;
            Ok(server)
        }
    }

    fn create_server(name: &str, first: bool) -> io::Result<NamedPipeServer> {
        let sid = current_user_sid_string()?;
        let descriptor = security_descriptor(&format!("D:P(A;;GA;;;{sid})"))?;
        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first)
            .reject_remote_clients(true);
        unsafe {
            options.create_with_security_attributes_raw(
                name,
                &mut attributes as *mut SECURITY_ATTRIBUTES as *mut c_void,
            )
        }
    }

    pub fn harden_private_directory(path: &Path) -> io::Result<()> {
        if !std::fs::metadata(path).is_ok_and(|metadata| metadata.is_dir()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "private data path must be an existing directory",
            ));
        }
        let sid = current_user_sid_string()?;
        let descriptor = security_descriptor(&format!("D:P(A;OICI;GA;;;{sid})"))?;
        let encoded_path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        let applied = unsafe {
            SetFileSecurityW(
                encoded_path.as_ptr(),
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                descriptor.0,
            )
        };
        if applied == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn security_descriptor(sddl: &str) -> io::Result<OwnedLocalMemory> {
        let encoded_sddl: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
        let mut descriptor_bytes = 0_u32;
        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                encoded_sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                &mut descriptor_bytes,
            )
        };
        if converted == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(OwnedLocalMemory(descriptor))
    }

    fn current_user_sid_string() -> io::Result<String> {
        let mut token: HANDLE = ptr::null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = OwnedHandle(token);
        let mut required = 0_u32;
        unsafe {
            GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut required);
        }
        if required < size_of::<TOKEN_USER>() as u32 {
            return Err(io::Error::last_os_error());
        }
        let word_bytes = size_of::<usize>();
        let words = (required as usize).div_ceil(word_bytes);
        let mut buffer = vec![0_usize; words];
        if unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                required,
                &mut required,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let user = unsafe { &*(buffer.as_ptr().cast::<TOKEN_USER>()) };
        let mut sid_text = ptr::null_mut();
        if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut sid_text) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let sid_text = OwnedLocalWideString(sid_text);
        let mut length = 0_usize;
        while length < 256 && unsafe { *sid_text.0.add(length) } != 0 {
            length += 1;
        }
        if length == 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "current user SID is too long",
            ));
        }
        String::from_utf16(unsafe { std::slice::from_raw_parts(sid_text.0, length) })
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "current user SID is invalid"))
    }

    struct OwnedHandle(HANDLE);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    struct OwnedLocalMemory(PSECURITY_DESCRIPTOR);

    impl Drop for OwnedLocalMemory {
        fn drop(&mut self) {
            unsafe {
                LocalFree(self.0);
            }
        }
    }

    struct OwnedLocalWideString(*mut u16);

    impl Drop for OwnedLocalWideString {
        fn drop(&mut self) {
            unsafe {
                LocalFree(self.0.cast());
            }
        }
    }

    fn normalize_pipe_name(endpoint: &Path) -> io::Result<String> {
        let raw = endpoint.to_string_lossy();
        let name = if raw.starts_with(r"\\.\pipe\") || raw.starts_with("//./pipe/") {
            raw.replace('/', "\\")
        } else {
            let leaf = endpoint
                .file_name()
                .and_then(|value| value.to_str())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "pipe endpoint is invalid")
                })?;
            if leaf.is_empty() || leaf.contains('\\') || leaf.contains('/') || leaf.contains("..") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "pipe name must be a single path segment",
                ));
            }
            format!(r"\\.\pipe\{leaf}")
        };
        if name.len() > 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pipe name is too long",
            ));
        }
        Ok(name)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn pipe_name_normalization_is_strict() {
            assert_eq!(
                normalize_pipe_name(Path::new(r"\\.\pipe\kt-signal-test")).unwrap(),
                r"\\.\pipe\kt-signal-test"
            );
            assert_eq!(
                normalize_pipe_name(Path::new("kt-signal-test")).unwrap(),
                r"\\.\pipe\kt-signal-test"
            );
            assert!(normalize_pipe_name(Path::new(r"a\b")).is_err());
        }
    }
}

pub use platform::{LocalListener, LocalStream};

#[cfg(windows)]
pub use platform::harden_private_directory;

/// Shared helper for packaging docs/tests: describe the platform endpoint shape.
pub fn endpoint_kind() -> &'static str {
    #[cfg(unix)]
    {
        "unix-socket"
    }
    #[cfg(windows)]
    {
        "named-pipe"
    }
}
