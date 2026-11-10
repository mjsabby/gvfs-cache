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
    /// A response did not arrive whole (its connection failed or closed before the body's
    /// end): the request can be made again.
    Incomplete,
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
    #[inline]
    // `chunks_exact`, as in `decode_hex`.
    #[allow(clippy::chunks_exact_to_as_chunks)]
    pub fn from_hex(hex: &[u8]) -> Option<Oid> {
        // Of a length known here, the five groups of eight digits are decoded in line: a
        // call of `decode_hex` (lengths it must check, loops it cannot unroll) was most
        // of what parsing an id of a request cost.
        let hex: &[u8; 40] = hex.try_into().ok()?;
        let mut out = [0u8; 20];
        let mut ok = true;
        for (o, digits) in out.chunks_exact_mut(4).zip(hex.chunks_exact(8)) {
            let (bytes, hex) = <[u8; 8]>::try_from(digits).map_or((0, false), hex8);
            o.copy_from_slice(&bytes.to_le_bytes());
            ok &= hex;
        }
        ok.then_some(Oid(out))
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

/// Eight hex digits (either case) as the four bytes they spell, and whether they all were
/// hex digits. The eight are handled as one `u64`, a byte per digit: an id's 40 digits
/// are five of these, decoded in line (5.2 ns an id), where the call of [`decode_hex`],
/// a digit at a time, took 14.3 ns.
#[inline(always)]
fn hex8(digits: [u8; 8]) -> (u32, bool) {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let x = u64::from_le_bytes(digits);
    // While every byte is below 0x80, adding a byte below 0x80 to each carries nothing
    // into the next: bit 7 of `byte + (0x80 - lo)` says the byte is at least `lo`.
    let from =
        |v: u64, lo: u8| v.wrapping_add(ONES.wrapping_mul(u64::from(0x80u8.wrapping_sub(lo))));
    let lower = x | ONES.wrapping_mul(0x20);
    let digit = from(x, b'0') & !from(x, b'9'.wrapping_add(1));
    let letter = from(lower, b'a') & !from(lower, b'f'.wrapping_add(1));
    let ok = x & HIGH == 0 && (digit | letter) & HIGH == HIGH;
    // A digit's value is its low nibble, a letter's that plus 9 (bit 6: letters only).
    let v = (x & ONES.wrapping_mul(0x0F)).wrapping_add((x.wrapping_shr(6) & ONES).wrapping_mul(9));
    // Two values to a byte (the first is the high nibble), then the four bytes together.
    let v = (v.wrapping_shl(4) | v.wrapping_shr(8)) & 0x00FF_00FF_00FF_00FF;
    let v = (v | v.wrapping_shr(8)) & 0x0000_FFFF_0000_FFFF;
    let v = v | v.wrapping_shr(16);
    // The low 32 bits: the mask makes the cast exact.
    ((v & 0xFFFF_FFFF) as u32, ok)
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

/// MurmurHash3's 64-bit finalizer: every bit of `k` reaches every bit of the result.
#[inline(always)]
pub fn fmix64(mut k: u64) -> u64 {
    k ^= k.wrapping_shr(33);
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k.wrapping_shr(33);
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ k.wrapping_shr(33)
}

/// A key drawn once per process from the system's randomness (the standard library's
/// hasher keys). Tables whose keys clients choose mix it in before placing a key, so that
/// which keys land together cannot be chosen from outside.
pub fn process_key() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static KEY: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *KEY.get_or_init(|| {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u32(std::process::id());
        h.finish()
    })
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

    /// An id is parsed eight digits at a time; it is what a digit at a time makes of the
    /// same 40 bytes, whatever they are: every byte value at every place, among digits of
    /// every kind, and bytes that are not ASCII beside ones that are.
    #[test]
    fn an_id_parses_as_its_digits_do_one_at_a_time() {
        let by_digit = |hex: &[u8]| {
            let mut out = [0u8; 20];
            decode_hex(hex, &mut out).then_some(Oid(out))
        };
        let mut s = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let digits = b"0123456789abcdefABCDEF";
        for round in 0..200 {
            let mut hex = [0u8; 40];
            for c in hex.iter_mut() {
                *c = digits[(next() % 22) as usize];
            }
            let oid = Oid::from_hex(&hex).unwrap();
            assert_eq!(Some(oid), by_digit(&hex));
            assert!(oid.to_hex().eq_ignore_ascii_case(&hex));
            for at in 0..40 {
                // A round's first places get every byte value, the others a few.
                let values: Vec<u8> = if round < 2 {
                    (0..=255).collect()
                } else {
                    (0..4).map(|_| next() as u8).collect()
                };
                for v in values {
                    let mut changed = hex;
                    changed[at] = v;
                    assert_eq!(Oid::from_hex(&changed), by_digit(&changed), "{changed:?}");
                    // And beside a byte that is not ASCII (a carry would cross into it).
                    changed[(at + 1) % 40] = 0x80 | next() as u8;
                    assert_eq!(Oid::from_hex(&changed), None, "{changed:?}");
                    assert_eq!(by_digit(&changed), None);
                }
            }
        }
        // Lengths other than 40.
        for len in [0, 1, 39, 41, 80] {
            assert_eq!(Oid::from_hex(&vec![b'a'; len]), None);
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
