// Based on
//
// <https://github.com/BurntSushi/ripgrep/blob/master/crates/ignore/src/pathutil.rs>
// Commit: 459a9c563706ef84b8710fab8727b770552ed29c

use std::ffi::OsStr;
use std::path::Path;

/// Returns true if and only if this entry is considered to be hidden.
///
/// This only returns true if the base name of the path starts with a `.`.
///
/// On Unix, this implements a more optimized check.
#[cfg(unix)]
pub fn is_hidden(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    if let Some(name) = file_name(path) {
        name.as_bytes().get(0) == Some(&b'.')
    } else {
        false
    }
}

// /// Returns true if and only if this entry is considered to be hidden.
// ///
// /// On Windows, this returns true if one of the following is true:
// ///
// /// * The base name of the path starts with a `.`.
// /// * The file attributes have the `HIDDEN` property set.
// #[cfg(windows)]
// pub fn is_hidden(dent: &DirEntry) -> bool {
//     use std::os::windows::fs::MetadataExt;
//     use winapi_util::file;

//     // This looks like we're doing an extra stat call, but on Windows, the
//     // directory traverser reuses the metadata retrieved from each directory
//     // entry and stores it on the DirEntry itself. So this is "free."
//     if let Ok(md) = dent.metadata() {
//         if file::is_hidden(md.file_attributes() as u64) {
//             return true;
//         }
//     }
//     if let Some(name) = file_name(dent.path()) {
//         name.to_str().map(|s| s.starts_with(".")).unwrap_or(false)
//     } else {
//         false
//     }
// }

// /// Returns true if and only if this entry is considered to be hidden.
// ///
// /// This only returns true if the base name of the path starts with a `.`.
// #[cfg(not(any(unix, windows)))]
// pub fn is_hidden(dent: &DirEntry) -> bool {
//     if let Some(name) = file_name(dent.path()) {
//         name.to_str().map(|s| s.starts_with(".")).unwrap_or(false)
//     } else {
//         false
//     }
// }

/// The final component of the path, if it is a normal file.
///
/// If the path terminates in ., .., or consists solely of a root of prefix,
/// file_name will return None.
#[cfg(unix)]
#[allow(clippy::if_same_then_else)]
pub fn file_name<P: AsRef<Path> + ?Sized>(path: &P) -> Option<&OsStr> {
    use memchr::memrchr;
    use std::os::unix::ffi::OsStrExt;

    let path = path.as_ref().as_os_str().as_bytes();
    if path.is_empty() {
        return None;
    } else if path.len() == 1 && path[0] == b'.' {
        return None;
    } else if path.last() == Some(&b'.') {
        return None;
    } else if path.len() >= 2 && path[path.len() - 2..] == b".."[..] {
        return None;
    }
    let last_slash = memrchr(b'/', path).map(|i| i + 1).unwrap_or(0);
    Some(OsStr::from_bytes(&path[last_slash..]))
}

// /// The final component of the path, if it is a normal file.
// ///
// /// If the path terminates in ., .., or consists solely of a root of prefix,
// /// file_name will return None.
// #[cfg(not(unix))]
// pub fn file_name<'a, P: AsRef<Path> + ?Sized>(path: &'a P) -> Option<&'a OsStr> {
//     path.as_ref().file_name()
// }
