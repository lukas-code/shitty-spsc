use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, Thread};

const INDEX_MASK: usize = 0b111;
const CLEANUP_INIT: usize = 0b1000;
const CLEANUP_DONE: usize = 0b10000;

const MAX_SPINS: u32 = 16;

/// Shared state between [`Producer`] and [`Consumer`].
///
/// | state | consumer | producer |
/// |-------|----------|----------|
/// |     0 |        1 |        2 |
/// |     1 |        1 |        0 |
/// |     2 |        2 |        0 |
/// |     3 |        2 |        1 |
/// |     4 |        0 |        1 |
/// |     5 |        0 |        2 |
struct Shared<T> {
    state: AtomicUsize,
    wake_me_up: AtomicPtr<Thread>,
    data: [UnsafeCell<VecDeque<T>>; 3],
}

impl<T> Shared<T> {
    const DATA_INIT: UnsafeCell<VecDeque<T>> = UnsafeCell::new(VecDeque::new());
    const INIT: Self = Self {
        state: AtomicUsize::new(0),
        wake_me_up: AtomicPtr::new(ptr::null_mut()),
        data: [Self::DATA_INIT; 3],
    };

    fn cleanup(&self) {
        self.state.fetch_or(CLEANUP_INIT, Ordering::Release);
    }

    #[cold]
    fn wake(&self) {
        let thread_ptr = self.wake_me_up.swap(ptr::null_mut(), Ordering::AcqRel);
        if !thread_ptr.is_null() {
            let thread = unsafe { Arc::from_raw(thread_ptr) };
            thread.unpark();
        }
    }
}

pub struct Producer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.shared.cleanup();
        self.shared.wake();
    }
}

pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    spin_count: Cell<u32>,
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.shared.cleanup();
    }
}

#[derive(Debug)]
pub struct SendError<T>(pub T);

#[derive(Debug)]
pub struct RecvError;

impl<T> Producer<T> {
    #[inline(never)]
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let shared = &*self.shared;

        let state = shared.state.load(Ordering::Acquire);

        if state >= CLEANUP_INIT {
            return Err(SendError(value));
        }

        let index = usize::from(state / 2);

        let vec = unsafe { &mut *shared.data.get_unchecked(index).get() };
        vec.push_back(value);

        if state & 1 == 0 {
            shared.state.fetch_or(1, Ordering::Release);
            self.shared.wake();
        }

        Ok(())
    }
}

impl<T> Consumer<T> {
    #[inline(never)]
    pub fn recv(&self) -> Result<T, RecvError> {
        let shared = &*self.shared;
        loop {
            let state = shared.state.load(Ordering::Acquire);
            let state_low = state & INDEX_MASK;
            let index = usize::from(((state_low / 2) + 1) % 3);

            let vec = unsafe { &mut *shared.data.get_unchecked(index).get() };
            if let Some(value) = vec.pop_front() {
                return Ok(value);
            } else if state & 1 != 0 {
                // bump consumer index
                let mask = state_low ^ ((state_low + 1) % 6);
                shared.state.fetch_xor(mask, Ordering::Release);
            } else if state >= CLEANUP_DONE {
                return Err(RecvError);
            } else if state >= CLEANUP_INIT {
                // last cleanup step
                let new_state = ((state_low + 2) % 6) | CLEANUP_INIT | CLEANUP_DONE;
                shared.state.store(new_state, Ordering::Relaxed);
            } else {
                self.wait(state);
            }
        }
    }

    #[cold]
    fn wait(&self, old_state: usize) {
        std::thread_local! {
            static THREAD: Arc<Thread> = Arc::new(thread::current());
        }

        let shared = &*self.shared;
        thread::yield_now();
        let spin_count = self.spin_count.get();
        if spin_count < MAX_SPINS {
            self.spin_count.set(spin_count + 1);
        } else {
            self.spin_count.set(0);
            THREAD.with(|thread| {
                let thread_ptr = Arc::into_raw(thread.clone()).cast_mut();
                let old_thread_ptr = shared.wake_me_up.swap(thread_ptr, Ordering::AcqRel);
                debug_assert!(old_thread_ptr.is_null());

                let next_state = shared.state.load(Ordering::Acquire);
                if old_state == next_state {
                    thread::park();
                }

                let thread_ptr = shared.wake_me_up.swap(ptr::null_mut(), Ordering::Relaxed);
                if !thread_ptr.is_null() {
                    drop(unsafe { Arc::from_raw(thread_ptr) });
                }
            });
        }
    }
}

impl<T> Iterator for Consumer<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}

pub fn channel<T>() -> (Producer<T>, Consumer<T>) {
    let shared = Arc::new(Shared::INIT);
    let producer = Producer {
        shared: shared.clone(),
    };
    let consumer = Consumer {
        shared,
        spin_count: Cell::new(0),
    };
    (producer, consumer)
}

// vorimplementierte Testsuite; bei Bedarf erweitern!

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::thread;

    use super::*;

    lazy_static! {
        static ref FOO_SET: Mutex<HashSet<i32>> = Mutex::new(HashSet::new());
    }

    #[derive(Debug)]
    struct Foo(i32);

    impl Foo {
        fn new(key: i32) -> Self {
            assert!(
                FOO_SET.lock().unwrap().insert(key),
                "double initialisation of element {}",
                key
            );
            Foo(key)
        }
    }

    impl Drop for Foo {
        fn drop(&mut self) {
            assert!(
                FOO_SET.lock().unwrap().remove(&self.0),
                "double free of element {}",
                self.0
            );
        }
    }

    // range of elements to be moved across the channel during testing
    const ELEMS: std::ops::Range<i32> = 0..1000;

    #[test]
    fn unused_elements_are_dropped() {
        lazy_static::initialize(&FOO_SET);

        for i in 0..if cfg!(miri) { 10 } else { 100 } {
            let (px, cx) = channel();
            let handle = thread::spawn(move || {
                for i in 0.. {
                    if px.send(Foo::new(i)).is_err() {
                        return;
                    }
                }
            });

            for _ in 0..i {
                cx.recv().unwrap();
            }

            drop(cx);

            assert!(handle.join().is_ok());

            let map = FOO_SET.lock().unwrap();
            if !map.is_empty() {
                panic!("FOO_MAP not empty: {:?}", *map);
            }
        }
    }

    #[test]
    fn elements_arrive_ordered() {
        for _ in 0..if cfg!(miri) { 10 } else { 100 } {
            let (px, cx) = channel();

            thread::spawn(move || {
                for i in ELEMS {
                    px.send(i).unwrap();
                }
            });

            for i in ELEMS {
                assert_eq!(i, cx.recv().unwrap());
            }

            assert!(cx.recv().is_err());
        }
    }

    #[test]
    fn all_elements_arrive() {
        for _ in 0..if cfg!(miri) { 10 } else { 100 } {
            let (px, cx) = channel();
            let handle = thread::spawn(move || {
                let mut count = 0;

                while let Ok(_) = cx.recv() {
                    count += 1;
                }

                count
            });

            thread::spawn(move || {
                for i in ELEMS {
                    px.send(i).unwrap();
                }
            });

            match handle.join() {
                Ok(count) => assert_eq!(count, ELEMS.len()),
                Err(_) => panic!("Error: join() returned Err"),
            }
        }
    }
}
