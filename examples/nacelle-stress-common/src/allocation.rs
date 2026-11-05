use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicU64, Ordering};

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_ZEROED_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REQUESTED_BYTES: AtomicU64 = AtomicU64::new(0);
static FREE_CALLS: AtomicU64 = AtomicU64::new(0);

/// Process-wide diagnostic allocator call counts, not live allocation sizes.
///
/// Reads use relaxed atomics and are not one atomic transaction. Snapshot at
/// quiescent measurement boundaries when possible. Counters wrap at `u64::MAX`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AllocationSnapshot {
    /// Successful calls to `GlobalAlloc::alloc`.
    pub alloc_calls: u64,
    /// Successful calls to `GlobalAlloc::alloc_zeroed`.
    pub alloc_zeroed_calls: u64,
    /// Successful calls to `GlobalAlloc::realloc`.
    pub realloc_calls: u64,
    /// Full requested sizes of successful allocations, including each realloc's new size.
    pub requested_bytes: u64,
    /// Calls to `GlobalAlloc::dealloc`; implicit frees inside realloc are not observable.
    pub free_calls: u64,
}

impl AllocationSnapshot {
    /// Returns successful alloc, zeroed-alloc, and realloc calls combined.
    pub fn successful_allocations(self) -> u64 {
        self.alloc_calls
            .wrapping_add(self.alloc_zeroed_calls)
            .wrapping_add(self.realloc_calls)
    }

    /// Returns counter deltas, saturating if an earlier snapshot exceeds this one.
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls.saturating_sub(earlier.alloc_calls),
            alloc_zeroed_calls: self
                .alloc_zeroed_calls
                .saturating_sub(earlier.alloc_zeroed_calls),
            realloc_calls: self.realloc_calls.saturating_sub(earlier.realloc_calls),
            requested_bytes: self.requested_bytes.saturating_sub(earlier.requested_bytes),
            free_calls: self.free_calls.saturating_sub(earlier.free_calls),
        }
    }
}

/// Reads process-wide counters from the diagnostic counting allocator.
pub fn allocation_snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
        alloc_zeroed_calls: ALLOC_ZEROED_CALLS.load(Ordering::Relaxed),
        realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
        requested_bytes: REQUESTED_BYTES.load(Ordering::Relaxed),
        free_calls: FREE_CALLS.load(Ordering::Relaxed),
    }
}

/// Delegates to the unchanged allocator and records diagnostic call counts.
///
/// Available only with `allocation-counting`. Instrumented builds add relaxed
/// atomic writes to allocation operations and must not be used as CPU baselines.
#[derive(Debug)]
pub struct CountingAllocator<A> {
    inner: A,
}

impl<A> CountingAllocator<A> {
    /// Wraps an allocator without changing its allocation or failure behavior.
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// SAFETY: Every operation forwards the original pointer, layout, and size to the
// wrapped allocator. Counters never inspect or modify the allocated memory.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller's GlobalAlloc requirements also apply to the delegate.
        let pointer = unsafe { self.inner.alloc(layout) };
        if !pointer.is_null() {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller's GlobalAlloc requirements also apply to the delegate.
        let pointer = unsafe { self.inner.alloc_zeroed(layout) };
        if !pointer.is_null() {
            ALLOC_ZEROED_CALLS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The original allocation and requested size are forwarded unchanged.
        let pointer = unsafe { self.inner.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The original allocation and layout are forwarded unchanged.
        unsafe { self.inner.dealloc(pointer, layout) };
        FREE_CALLS.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::System;
    use std::sync::atomic::AtomicBool;

    struct FallibleAllocator {
        fail: AtomicBool,
    }

    // SAFETY: Successful operations delegate unchanged to System. Failed
    // allocations return null, and failed reallocations retain the original allocation.
    unsafe impl GlobalAlloc for FallibleAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if self.fail.load(Ordering::Relaxed) {
                std::ptr::null_mut()
            } else {
                // SAFETY: The caller supplies a valid allocation layout.
                unsafe { System.alloc(layout) }
            }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if self.fail.load(Ordering::Relaxed) {
                std::ptr::null_mut()
            } else {
                // SAFETY: The caller supplies a valid allocation layout.
                unsafe { System.alloc_zeroed(layout) }
            }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if self.fail.load(Ordering::Relaxed) {
                std::ptr::null_mut()
            } else {
                // SAFETY: The caller supplies the original allocation and a valid new size.
                unsafe { System.realloc(pointer, layout, new_size) }
            }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            // SAFETY: The caller supplies the original allocation and layout.
            unsafe { System.dealloc(pointer, layout) };
        }
    }

    #[test]
    fn counts_successful_calls_and_full_requested_sizes_but_not_failures() {
        let allocator = CountingAllocator::new(FallibleAllocator {
            fail: AtomicBool::new(false),
        });
        let first_layout = Layout::from_size_align(16, 8).expect("valid layout");
        let zeroed_layout = Layout::from_size_align(8, 8).expect("valid layout");
        let resized_layout = Layout::from_size_align(32, 8).expect("valid layout");
        let before = allocation_snapshot();

        // SAFETY: Layouts are nonzero and valid. Every successful allocation is
        // freed once with its current layout, including after a failed realloc.
        unsafe {
            let pointer = allocator.alloc(first_layout);
            assert!(!pointer.is_null());
            let zeroed = allocator.alloc_zeroed(zeroed_layout);
            assert!(!zeroed.is_null());
            assert_eq!(*zeroed, 0);
            let resized = allocator.realloc(pointer, first_layout, 32);
            assert!(!resized.is_null());

            allocator.inner.fail.store(true, Ordering::Relaxed);
            assert!(allocator.alloc(first_layout).is_null());
            assert!(allocator.alloc_zeroed(zeroed_layout).is_null());
            assert!(allocator.realloc(resized, resized_layout, 64).is_null());
            allocator.dealloc(resized, resized_layout);
            allocator.dealloc(zeroed, zeroed_layout);
        }

        assert_eq!(
            allocation_snapshot().delta_since(before),
            AllocationSnapshot {
                alloc_calls: 1,
                alloc_zeroed_calls: 1,
                realloc_calls: 1,
                requested_bytes: 56,
                free_calls: 2,
            }
        );
    }
}
