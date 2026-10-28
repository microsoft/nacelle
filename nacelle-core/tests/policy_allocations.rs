use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use nacelle_core::policy::{Admission, Chain, NoPolicy, check_fn};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

// Each operation forwards the unchanged allocation contract to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(value) = count.get() {
                count.set(Some(value + 1));
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("allocation fixture must remain ready"),
    }
}

#[test]
fn empty_and_composed_ready_admission_do_not_allocate() {
    let policies = Chain::new(
        check_fn(|value: &Cell<usize>| {
            value.set(value.get() + 1);
            Ok(())
        }),
        check_fn(|value: &Cell<usize>| {
            value.set(value.get() + 2);
            Ok(())
        }),
    );
    let mut input = Cell::new(0);

    ALLOCATIONS.with(|count| count.set(Some(0)));
    let empty = ready(Admission::enter(&NoPolicy, &mut input));
    let composed = ready(Admission::enter(&policies, &mut input));
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());

    assert!(empty.is_ok());
    assert!(composed.is_ok());
    assert_eq!(input.get(), 3);
    assert_eq!(count, 0);
}
