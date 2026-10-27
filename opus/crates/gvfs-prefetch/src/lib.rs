//! `gvfs-prefetch` — a hint to start loading a cache line.
//!
//! Batched lookups (thousands of ids per request) miss the cache once per id, and each miss
//! costs a trip to DRAM. Asking for the line of an id some way ahead of the one being looked
//! up lets those trips overlap. A prefetch is only a hint: it never faults, never reads into
//! anything the program sees, and the CPU may ignore it, so it cannot change a result.
//!
//! x86-64 asks with `prefetcht0` (`_mm_prefetch`), ARM64 with `prfm pldl1keep` (inline
//! assembly: the ARM64 prefetch intrinsic is not stable). Elsewhere the hint is nothing.

/// Starts loading the cache line that holds `bytes[at]`; nothing if `at` is out of range.
#[inline(always)]
#[allow(unsafe_code)]
pub fn prefetch(bytes: &[u8], at: usize) {
    #[cfg(target_arch = "x86_64")]
    if let Some(b) = bytes.get(at..) {
        // SAFETY: `_mm_prefetch` needs SSE, which every x86-64 CPU has (it is in the target's
        // baseline), and it never dereferences its argument, here a pointer into `bytes`.
        unsafe {
            core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(
                b.as_ptr().cast(),
            )
        }
    }
    #[cfg(target_arch = "aarch64")]
    if let Some(b) = bytes.get(at..) {
        // SAFETY: `prfm` is in the base A64 instruction set, so every ARM64 CPU has it. It is
        // a hint: it never faults (not even on an unmapped address), writes no register, no
        // memory and no flag, and here gets a pointer into `bytes`.
        unsafe {
            core::arch::asm!(
                "prfm pldl1keep, [{addr}]",
                addr = in(reg) b.as_ptr(),
                options(nostack, preserves_flags, readonly)
            )
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = (bytes, at);
}
