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

    /// `bind(2)` copies the socket path into `sockaddr_un::sun_path`, a
    /// fixed-size array: 104 bytes on macOS (`sys/socket.h`) and 108 on
    /// Linux (`bits/socket.h`), and the path plus its terminating NUL must
    /// fit, leaving 103/107 usable bytes. An overrun surfaces from
    /// `bind(2)` as `EINVAL` — synchronous, non-retryable, and naming
    /// nothing (optimization-plan §5.5 root cause) — so it is rejected
    /// here with a readable error instead. Both the published endpoint and
    /// the per-process staging name it is first bound under must fit; see
    /// [`staging_path`].
    const MAX_SUN_PATH_BYTES: usize = if cfg!(target_os = "macos") { 103 } else { 107 };

    /// Reject a path that `bind(2)` would fail on with a bare `EINVAL`.
    /// `described` distinguishes the endpoint from its staging name in the
    /// diagnostic; the path itself is never echoed (AGENTS.md log
    /// discipline).
    fn ensure_bindable_length(path: &Path, described: &str) -> io::Result<()> {
        let length = path.as_os_str().as_encoded_bytes().len();
        if length > MAX_SUN_PATH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{described} is {length} bytes; this platform's unix-socket \
                     path limit is {MAX_SUN_PATH_BYTES} bytes — use a shorter \
                     endpoint directory"
                ),
            ));
        }
        Ok(())
    }

    pub struct LocalListener {
        inner: UnixListener,
        path: PathBuf,
        device: u64,
        inode: u64,
    }

    impl LocalListener {
        pub fn bind(endpoint: &Path) -> io::Result<Self> {
            let path = prepare_endpoint(endpoint)?;
            // Bind under a staging name and publish with a rename. Binding
            // directly would make the endpoint visible with the umask's mode
            // for as long as it takes to harden it, and the host connects as
            // soon as the path appears, so that window is reachable.
            let staging = staging_path(&path)?;
            let _ = fs::remove_file(&staging);
            let inner = UnixListener::bind(&staging)?;
            let published = fs::set_permissions(&staging, fs::Permissions::from_mode(0o600))
                .and_then(|()| fs::rename(&staging, &path))
                .and_then(|()| fs::symlink_metadata(&path));
            let metadata = match published {
                Ok(metadata) => metadata,
                Err(error) => {
                    let _ = fs::remove_file(&staging);
                    return Err(error);
                }
            };
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

    /// A sibling of the endpoint, in the same private directory so the rename
    /// stays on one filesystem, and per-process so two starts cannot collide.
    fn staging_path(path: &Path) -> io::Result<PathBuf> {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "endpoint must include a socket file name",
                )
            })?;
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "endpoint must include a parent directory",
            )
        })?;
        let staging = parent.join(format!(".{file_name}.{}.staging", std::process::id()));
        // The staging name is what bind(2) actually sees first, and it is
        // always longer than the published endpoint; check it against the
        // same sun_path limit so an endpoint that only fits without its
        // staging suffix still fails fast and readably.
        ensure_bindable_length(&staging, "staging path of the endpoint")?;
        Ok(staging)
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
        ensure_bindable_length(&path, "endpoint path")?;
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

        #[tokio::test]
        async fn the_endpoint_is_only_ever_visible_as_a_private_socket() {
            let temp = tempfile::TempDir::new().unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let endpoint = temp.path().join("connector.sock");

            let listener = LocalListener::bind(&endpoint).unwrap();

            // The host polls for this path and connects the moment it appears,
            // so the very first thing anyone can observe must already be 0600.
            let metadata = fs::symlink_metadata(&endpoint).unwrap();
            assert!(metadata.file_type().is_socket());
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            // Staging left nothing behind.
            let leftovers: Vec<_> = fs::read_dir(temp.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .filter(|name| name != "connector.sock")
                .collect();
            assert!(leftovers.is_empty(), "unexpected leftovers: {leftovers:?}");
            drop(listener);
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

        /// ADR 0002 guard: a second listener on the same endpoint path must
        /// fail closed (first-bind-wins), which is what keeps two connector
        /// instances from sharing an endpoint namespace by accident.
        #[tokio::test]
        async fn a_second_listener_on_the_same_endpoint_is_rejected() {
            let temp = tempfile::TempDir::new().unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let endpoint = temp.path().join("connector.sock");
            let _first = LocalListener::bind(&endpoint).unwrap();
            match LocalListener::bind(&endpoint) {
                Ok(_) => panic!("a second listener on the same endpoint must be rejected"),
                Err(error) => assert_eq!(error.kind(), io::ErrorKind::AlreadyExists),
            }
        }

        /// ADR 0002 guard: an endpoint path over the platform sun_path
        /// limit must be rejected at startup with a readable error naming
        /// the limit, instead of the historical bare `EINVAL` from bind(2)
        /// (optimization-plan §5.5 root cause).
        #[test]
        fn an_endpoint_path_over_the_sun_path_limit_is_rejected_readably() {
            let temp = tempfile::TempDir::new().unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let parent = fs::canonicalize(temp.path()).unwrap();
            // Size the file name so the canonical endpoint path is exactly
            // one byte over the limit.
            let base = parent.as_os_str().as_encoded_bytes().len();
            // `join` adds one separator byte, so a name of MAX-base-1
            // characters makes the canonical path exactly MAX bytes and
            // MAX-base makes it one byte over.
            assert!(
                base + 2 <= MAX_SUN_PATH_BYTES,
                "test environment temp path is unusually long ({base} bytes)"
            );
            let endpoint = parent.join("s".repeat(MAX_SUN_PATH_BYTES - base));
            match LocalListener::bind(&endpoint) {
                Ok(_) => panic!("an over-limit endpoint must be rejected"),
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                    let message = error.to_string();
                    assert!(
                        message.contains(&MAX_SUN_PATH_BYTES.to_string()),
                        "error must name the limit: {message}"
                    );
                    assert!(
                        message.contains("unix-socket path limit"),
                        "error must say what limit was hit: {message}"
                    );
                }
            }
        }

        /// Exactly at the limit the published endpoint itself fits, but
        /// bind(2) happens on the longer per-process staging name
        /// (`.<name>.<pid>.staging`) first — the exact shape of the
        /// historical EINVAL fast-fail. The staging name must fit too.
        #[test]
        fn an_endpoint_at_the_limit_is_rejected_when_its_staging_name_does_not_fit() {
            let temp = tempfile::TempDir::new().unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let parent = fs::canonicalize(temp.path()).unwrap();
            let base = parent.as_os_str().as_encoded_bytes().len();
            assert!(
                base + 2 <= MAX_SUN_PATH_BYTES,
                "test environment temp path is unusually long ({base} bytes)"
            );
            let endpoint = parent.join("s".repeat(MAX_SUN_PATH_BYTES - base - 1));
            match LocalListener::bind(&endpoint) {
                Ok(_) => panic!("an endpoint whose staging name cannot fit must be rejected"),
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                    let message = error.to_string();
                    assert!(
                        message.contains("staging path"),
                        "error must name the staging path: {message}"
                    );
                }
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
        // Microsoft documents the entire named-pipe name string as limited
        // to 256 characters (CreateNamedPipe lpName). We count the whole
        // normalized `\\.\pipe\...` string, prefix included, in UTF-8 bytes
        // — equal to the documented bound for ASCII names and stricter for
        // non-ASCII ones. There is no separate per-segment limit to check:
        // the single-path-segment rule above is the only other shape
        // constraint the API documents.
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

        /// ADR 0002 guard: the documented 256-character pipe-name limit is
        /// enforced fail-fast (256 total is accepted, 257 is not).
        #[test]
        fn overlong_pipe_names_are_rejected_fail_fast() {
            // `\\.\pipe\` is 9 characters, so 247 more reach exactly 256.
            let at_limit = format!(r"\\.\pipe\{}", "p".repeat(247));
            assert_eq!(
                normalize_pipe_name(Path::new(&at_limit)).unwrap().len(),
                256
            );
            let over_limit = format!(r"\\.\pipe\{}", "p".repeat(248));
            let error = normalize_pipe_name(Path::new(&over_limit)).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(error.to_string(), "pipe name is too long");
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
