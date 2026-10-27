//! Small shared pieces: the error type, object ids, hex, byte readers, time and logging.

use std::fmt;

/// One error type for the whole binary: a message with context prepended as it bubbles up.
pub struct Error {
    msg: String,
    pub kind: ErrorKind,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ErrorKind {
    Other,
    /// A conditional write lost the race (HTTP 412 / create-only target exists).
    Conflict,
    /// The object does not exist.
    NotFound,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Runs `f` in a cold section: errors are built here, so that a failing request (a
/// malformed body, an over-capacity batch) never counts as a hot-path allocation (or, under
/// `--alloc-guard strict`, aborts the edge).
#[inline]
pub fn cold<R>(f: impl FnOnce() -> R) -> R {
    let _cold = gvfs_alloc_guard::cold_section();
    f()
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Error {
        cold(|| Error {
            msg: msg.into(),
            kind: ErrorKind::Other,
        })
    }

    pub fn with_kind(kind: ErrorKind, msg: impl Into<String>) -> Error {
        cold(|| Error {
            msg: msg.into(),
            kind,
        })
    }

    pub fn is_conflict(&self) -> bool {
        self.kind == ErrorKind::Conflict
    }

    pub fn is_not_found(&self) -> bool {
        self.kind == ErrorKind::NotFound
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        let kind = if e.kind() == std::io::ErrorKind::NotFound {
            ErrorKind::NotFound
        } else {
            ErrorKind::Other
        };
        cold(|| Error::with_kind(kind, e.to_string()))
    }
}

impl From<nix::Error> for Error {
    fn from(e: nix::Error) -> Error {
        cold(|| Error::new(e.to_string()))
    }
}

/// `return Err(Error::new(format!(..)))`.
#[macro_export]
macro_rules! bail {
    ($($t:tt)*) => { return Err($crate::util::cold(|| $crate::util::Error::new(format!($($t)*)))) };
}

/// `Error::new(format!(..))`.
#[macro_export]
macro_rules! err {
    ($($t:tt)*) => { $crate::util::cold(|| $crate::util::Error::new(format!($($t)*))) };
}

/// Prepends context to an error: `file.read(..).ctx("reading manifest")?`.
pub trait Context<T> {
    fn ctx(self, what: &str) -> Result<T>;
}

impl<T, E: Into<Error>> Context<T> for std::result::Result<T, E> {
    fn ctx(self, what: &str) -> Result<T> {
        self.map_err(|e| {
            let _cold = gvfs_alloc_guard::cold_section();
            let e: Error = e.into();
            Error {
                msg: format!("{what}: {}", e.msg),
                kind: e.kind,
            }
        })
    }
}

impl<T> Context<T> for Option<T> {
    fn ctx(self, what: &str) -> Result<T> {
        self.ok_or_else(|| Error::new(what))
    }
}

/// One line on stderr, prefixed with the Unix time.
#[macro_export]
macro_rules! log {
    ($($t:tt)*) => {{
        eprintln!("[{:.3}] {}", $crate::util::now_secs_f64(), format_args!($($t)*));
    }};
}

// ---------------------------------------------------------------------------------------
// Object ids and hex
// ---------------------------------------------------------------------------------------

/// A SHA-1 git object id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Oid(pub [u8; 20]);

impl Oid {
    pub const ZERO: Oid = Oid([0u8; 20]);

    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 20]
    }

    /// Parses exactly 40 hex digits (either case).
    pub fn from_hex(hex: &[u8]) -> Option<Oid> {
        let mut out = [0u8; 20];
        if decode_hex(hex, &mut out) {
            Some(Oid(out))
        } else {
            None
        }
    }

    pub fn from_slice(b: &[u8]) -> Option<Oid> {
        Some(Oid(b.try_into().ok()?))
    }

    pub fn to_hex(&self) -> [u8; 40] {
        let mut out = [0u8; 40];
        encode_hex(&self.0, &mut out);
        out
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = self.to_hex();
        f.write_str(std::str::from_utf8(&hex).unwrap_or("?"))
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Value of one hex digit, or 0xFF. Branch-free so loops over it vectorize.
#[inline(always)]
pub fn hex_value(c: u8) -> u8 {
    let digit = c.wrapping_sub(b'0');
    let alpha = (c | 0x20).wrapping_sub(b'a');
    let is_digit = u8::from(digit < 10);
    let is_alpha = u8::from(alpha < 6);
    let neither = (is_digit | is_alpha) ^ 1;
    (is_digit.wrapping_neg() & digit)
        | (is_alpha.wrapping_neg() & alpha.wrapping_add(10))
        | neither.wrapping_neg()
}

/// Decodes `2 * out.len()` hex digits. Returns false on a length mismatch or any non-hex byte.
#[inline]
// `chunks_exact`, not the `as_chunks` clippy prefers: with `as_chunks` here and in
// `encode_hex`, a 4,000-id sizes request costs the edge 3% more CPU (REPORT.md §16).
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn decode_hex(hex: &[u8], out: &mut [u8]) -> bool {
    if Some(hex.len()) != out.len().checked_mul(2) {
        return false;
    }
    let mut bad = 0u8;
    for (o, pair) in out.iter_mut().zip(hex.chunks_exact(2)) {
        if let [h, l] = *pair {
            let hi = hex_value(h);
            let lo = hex_value(l);
            bad |= hi | lo;
            *o = hi.wrapping_shl(4) | (lo & 0x0F);
        }
    }
    bad & 0xF0 == 0
}

#[inline(always)]
fn hex_digit(nibble: u8) -> u8 {
    let d = nibble & 0x0F;
    // '0'..'9' then 'a'..'f' ('0' + 10 + 39 == 'a').
    d.wrapping_add(b'0')
        .wrapping_add(if d > 9 { 39 } else { 0 })
}

/// Lowercase hex of `bytes` into `out` (`out.len() == 2 * bytes.len()`).
#[inline]
// `chunks_exact_mut`, as in `decode_hex`: LLVM vectorizes this form better (5.1 ns per id
// instead of 6.0 with `as_chunks_mut`).
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn encode_hex(bytes: &[u8], out: &mut [u8]) {
    for (pair, &b) in out.chunks_exact_mut(2).zip(bytes) {
        if let [h, l] = pair {
            *h = hex_digit(b.wrapping_shr(4));
            *l = hex_digit(b);
        }
    }
}

pub fn hex_string(bytes: &[u8]) -> String {
    let mut out = vec![0u8; bytes.len().saturating_mul(2)];
    encode_hex(bytes, &mut out);
    String::from_utf8(out).unwrap_or_default()
}

// ---------------------------------------------------------------------------------------
// Bounds-checked little readers (never panic; `None` past the end)
// ---------------------------------------------------------------------------------------

#[inline(always)]
pub fn le_u16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        b.get(at..at.checked_add(2)?)?.try_into().ok()?,
    ))
}

#[inline(always)]
pub fn le_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        b.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

#[inline(always)]
pub fn le_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        b.get(at..at.checked_add(8)?)?.try_into().ok()?,
    ))
}

#[inline(always)]
pub fn be_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        b.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

/// `b[at..at + len]` without panicking.
#[inline(always)]
pub fn sub(b: &[u8], at: usize, len: usize) -> Option<&[u8]> {
    b.get(at..at.checked_add(len)?)
}

/// `u64` byte offset to `usize` (lossless on the 64-bit targets we build for).
#[inline(always)]
pub fn us(x: u64) -> usize {
    usize::try_from(x).unwrap_or(usize::MAX)
}

/// `usize` to `u64` (lossless on the 64-bit targets we build for).
#[inline(always)]
pub fn u64_of(x: usize) -> u64 {
    u64::try_from(x).unwrap_or(u64::MAX)
}

/// `usize` to `u32`, saturating.
#[inline(always)]
pub fn u32_of(x: usize) -> u32 {
    u32::try_from(x).unwrap_or(u32::MAX)
}

/// High 64 bits of the 128-bit product: maps a uniform `h` onto `0..n` without division.
#[inline(always)]
pub fn mul_hi(h: u64, n: u64) -> u64 {
    let wide = u128::from(h).wrapping_mul(u128::from(n));
    u64::try_from(wide.wrapping_shr(64)).unwrap_or(0)
}

/// Appends `data` to `v` only if it fits in the existing capacity (never reallocates).
#[inline]
pub fn try_extend(v: &mut Vec<u8>, data: &[u8]) -> bool {
    if v.capacity().saturating_sub(v.len()) < data.len() {
        return false;
    }
    v.extend_from_slice(data);
    true
}

/// Starts loading the cache line that holds `bytes[at]` (nothing if `at` is out of range).
/// Batched lookups issue this for an id some way ahead of the one they are on, so that the
/// cache misses of a batch overlap instead of following one another.
#[inline(always)]
pub fn prefetch(bytes: &[u8], at: usize) {
    gvfs_prefetch::prefetch(bytes, at);
}

/// Pushes `item` only if it fits in the existing capacity (never reallocates).
#[inline]
pub fn try_push<T>(v: &mut Vec<T>, item: T) -> bool {
    if v.len() >= v.capacity() {
        return false;
    }
    v.push(item);
    true
}

/// `v` in decimal ASCII, written right-aligned into `buf`; returns the digits.
#[inline]
pub fn decimal(mut v: u64, buf: &mut [u8; 20]) -> &[u8] {
    let mut i = buf.len();
    loop {
        i = i.saturating_sub(1);
        if let Some(slot) = buf.get_mut(i) {
            // `v % 10 < 10`, so the cast cannot truncate.
            *slot = b'0'.wrapping_add(v.wrapping_rem(10) as u8);
        }
        v = v.wrapping_div(10);
        if v == 0 || i == 0 {
            break;
        }
    }
    buf.get(i..).unwrap_or(&[])
}

// ---------------------------------------------------------------------------------------
// Time, formatting, threads
// ---------------------------------------------------------------------------------------

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn now_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Formats a byte count for logs.
/// This process's peak resident set size in bytes (its own, not its children's).
pub fn peak_rss() -> u64 {
    nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
        .map(|u| u64::try_from(u.max_rss()).unwrap_or(0).saturating_mul(1024))
        .unwrap_or(0)
}

pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut unit = "B";
    for u in UNITS.iter().skip(1) {
        if v < 1024.0 {
            break;
        }
        v /= 1024.0;
        unit = u;
    }
    if unit == "B" {
        format!("{n} B")
    } else {
        format!("{v:.2} {unit}")
    }
}

/// Number of worker threads to use for parallel batch work.
pub fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_formats_like_display() {
        let mut buf = [0u8; 20];
        for v in [0u64, 7, 9, 10, 99, 100, 4167, 1 << 40, u64::MAX] {
            assert_eq!(decimal(v, &mut buf), v.to_string().as_bytes());
        }
    }

    #[test]
    fn hex_roundtrip_and_rejects() {
        let oid = Oid::from_hex(b"0123456789abcdefABCDEF0123456789abcdef01").unwrap();
        assert_eq!(&oid.to_hex(), b"0123456789abcdefabcdef0123456789abcdef01");
        for bad in [
            &b"g123456789abcdefABCDEF0123456789abcdef01"[..],
            b"0123",
            b"0123456789abcdefABCDEF0123456789abcdef0:",
        ] {
            assert!(Oid::from_hex(bad).is_none());
        }
        for c in 0u8..=255 {
            let expect = (c as char).to_digit(16).map(|d| d as u8).unwrap_or(0xFF);
            assert_eq!(hex_value(c), expect, "{c}");
        }
        for n in 0u8..16 {
            assert_eq!(hex_digit(n), b"0123456789abcdef"[n as usize]);
        }
    }

    #[test]
    fn readers_and_mul_hi() {
        let b = [1u8, 0, 0, 0, 2, 0, 0, 0, 3];
        assert_eq!(le_u32(&b, 0), Some(1));
        assert_eq!(le_u64(&b, 0), Some(0x0000_0002_0000_0001));
        assert_eq!(le_u64(&b, 2), None);
        assert_eq!(sub(&b, 8, 1), Some(&[3u8][..]));
        assert_eq!(sub(&b, 8, 2), None);
        assert_eq!(mul_hi(u64::MAX, 10), 9);
        assert_eq!(mul_hi(0, 10), 0);
    }
}
