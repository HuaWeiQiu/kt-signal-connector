// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(windows)]
pub async fn wait_for_parent_exit(parent_pid: u32) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || wait_for_parent_exit_blocking(parent_pid))
        .await
        .map_err(|_| std::io::Error::other("parent monitor task failed"))?
}

#[cfg(windows)]
fn wait_for_parent_exit_blocking(parent_pid: u32) -> std::io::Result<()> {
    use std::ptr;

    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Threading::{
        INFINITE, OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, parent_pid) };
    if process == ptr::null_mut() {
        return Err(std::io::Error::last_os_error());
    }
    let result = unsafe { WaitForSingleObject(process, INFINITE) };
    unsafe {
        CloseHandle(process);
    }
    if result != WAIT_OBJECT_0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
