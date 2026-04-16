//! Future for submitting linked operation pairs to the runtime.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use compio_buf::BufResult;
use compio_driver::{Key, OpCode, PushEntry};
use pin_project_lite::pin_project;

use crate::runtime::Runtime;

pin_project! {
    /// Future that submits two operations as an io_uring linked chain.
    ///
    /// The kernel executes the second operation only after the first
    /// completes successfully. If the first fails, the second is
    /// cancelled (`-ECANCELED`).
    ///
    /// Resolves when **both** operations have completed.
    pub struct SubmitLinkedPair<T1: OpCode, T2: OpCode> {
        runtime: Runtime,
        state: Option<LinkedState<T1, T2>>,
    }

    impl<T1: OpCode, T2: OpCode> PinnedDrop for SubmitLinkedPair<T1, T2> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(state) = this.state.take() {
                match state {
                    LinkedState::Submitted { key1, key2, .. } => {
                        this.runtime.cancel(key1);
                        this.runtime.cancel(key2);
                    }
                    LinkedState::FirstDone { key2, .. } => {
                        this.runtime.cancel(key2);
                    }
                    LinkedState::SecondDone { key1, .. } => {
                        this.runtime.cancel(key1);
                    }
                    _ => {}
                }
            }
        }
    }
}

enum LinkedState<T1: OpCode, T2: OpCode> {
    Idle { op1: T1, op2: T2 },
    Submitted { key1: Key<T1>, key2: Key<T2> },
    FirstDone {
        result1: BufResult<usize, T1>,
        key2: Key<T2>,
    },
    SecondDone {
        key1: Key<T1>,
        result2: BufResult<usize, T2>,
    },
}

impl<T1: OpCode + 'static, T2: OpCode + 'static> SubmitLinkedPair<T1, T2> {
    pub(crate) fn new(runtime: Runtime, op1: T1, op2: T2) -> Self {
        Self {
            runtime,
            state: Some(LinkedState::Idle { op1, op2 }),
        }
    }
}

impl<T1: OpCode + 'static, T2: OpCode + 'static> Future for SubmitLinkedPair<T1, T2> {
    type Output = io::Result<(BufResult<usize, T1>, BufResult<usize, T2>)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        loop {
            match this.state.take().expect("Cannot poll after ready") {
                LinkedState::Idle { op1, op2 } => {
                    let result = this
                        .runtime
                        .driver
                        .borrow_mut()
                        .push_linked_pair(op1, op2);

                    match result {
                        Ok((key1, key2)) => {
                            *this.state = Some(LinkedState::Submitted { key1, key2 });
                        }
                        Err(e) => {
                            return Poll::Ready(Err(e));
                        }
                    }
                }
                LinkedState::Submitted { key1, key2 } => {
                    let res1 = this.runtime.poll_task(cx.waker(), key1);
                    let res2 = this.runtime.poll_task(cx.waker(), key2);

                    match (res1, res2) {
                        (PushEntry::Ready(r1), PushEntry::Ready(r2)) => {
                            return Poll::Ready(Ok((r1, r2)));
                        }
                        (PushEntry::Ready(r1), PushEntry::Pending(k2)) => {
                            *this.state = Some(LinkedState::FirstDone {
                                result1: r1,
                                key2: k2,
                            });
                            return Poll::Pending;
                        }
                        (PushEntry::Pending(k1), PushEntry::Ready(r2)) => {
                            *this.state = Some(LinkedState::SecondDone {
                                key1: k1,
                                result2: r2,
                            });
                            return Poll::Pending;
                        }
                        (PushEntry::Pending(k1), PushEntry::Pending(k2)) => {
                            *this.state = Some(LinkedState::Submitted {
                                key1: k1,
                                key2: k2,
                            });
                            return Poll::Pending;
                        }
                    }
                }
                LinkedState::FirstDone { result1, key2 } => {
                    match this.runtime.poll_task(cx.waker(), key2) {
                        PushEntry::Ready(r2) => return Poll::Ready(Ok((result1, r2))),
                        PushEntry::Pending(k2) => {
                            *this.state = Some(LinkedState::FirstDone {
                                result1,
                                key2: k2,
                            });
                            return Poll::Pending;
                        }
                    }
                }
                LinkedState::SecondDone { key1, result2 } => {
                    match this.runtime.poll_task(cx.waker(), key1) {
                        PushEntry::Ready(r1) => return Poll::Ready(Ok((r1, result2))),
                        PushEntry::Pending(k1) => {
                            *this.state = Some(LinkedState::SecondDone {
                                key1: k1,
                                result2,
                            });
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SubmitLinkedTriple: 3-SQE linked chain
// ---------------------------------------------------------------------------

pin_project! {
    /// Future that submits three operations as an io_uring linked chain.
    pub struct SubmitLinkedTriple<T1: OpCode, T2: OpCode, T3: OpCode> {
        runtime: Runtime,
        state: Option<TripleState<T1, T2, T3>>,
    }

    impl<T1: OpCode, T2: OpCode, T3: OpCode> PinnedDrop for SubmitLinkedTriple<T1, T2, T3> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(state) = this.state.take() {
                cancel_triple_keys(&this.runtime, state);
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum TripleState<T1: OpCode, T2: OpCode, T3: OpCode> {
    Idle { op1: T1, op2: T2, op3: T3 },
    Submitted { key1: Key<T1>, key2: Key<T2>, key3: Key<T3> },
    // One done (3 variants)
    Done1 { r1: BufResult<usize, T1>, key2: Key<T2>, key3: Key<T3> },
    Done2 { key1: Key<T1>, r2: BufResult<usize, T2>, key3: Key<T3> },
    Done3 { key1: Key<T1>, key2: Key<T2>, r3: BufResult<usize, T3> },
    // Two done (3 variants)
    Done12 { r1: BufResult<usize, T1>, r2: BufResult<usize, T2>, key3: Key<T3> },
    Done13 { r1: BufResult<usize, T1>, key2: Key<T2>, r3: BufResult<usize, T3> },
    Done23 { key1: Key<T1>, r2: BufResult<usize, T2>, r3: BufResult<usize, T3> },
}

/// Cancel all outstanding keys in a TripleState.
fn cancel_triple_keys<T1: OpCode, T2: OpCode, T3: OpCode>(
    rt: &Runtime,
    state: TripleState<T1, T2, T3>,
) {
    match state {
        TripleState::Submitted { key1, key2, key3 } => {
            rt.cancel(key1); rt.cancel(key2); rt.cancel(key3);
        }
        TripleState::Done1 { key2, key3, .. } => { rt.cancel(key2); rt.cancel(key3); }
        TripleState::Done2 { key1, key3, .. } => { rt.cancel(key1); rt.cancel(key3); }
        TripleState::Done3 { key1, key2, .. } => { rt.cancel(key1); rt.cancel(key2); }
        TripleState::Done12 { key3, .. } => { rt.cancel(key3); }
        TripleState::Done13 { key2, .. } => { rt.cancel(key2); }
        TripleState::Done23 { key1, .. } => { rt.cancel(key1); }
        TripleState::Idle { .. } => {}
    }
}

impl<T1: OpCode + 'static, T2: OpCode + 'static, T3: OpCode + 'static>
    SubmitLinkedTriple<T1, T2, T3>
{
    pub(crate) fn new(runtime: Runtime, op1: T1, op2: T2, op3: T3) -> Self {
        Self {
            runtime,
            state: Some(TripleState::Idle { op1, op2, op3 }),
        }
    }
}

type TripleOutput<T1, T2, T3> =
    io::Result<(BufResult<usize, T1>, BufResult<usize, T2>, BufResult<usize, T3>)>;

impl<T1: OpCode + 'static, T2: OpCode + 'static, T3: OpCode + 'static> Future
    for SubmitLinkedTriple<T1, T2, T3>
{
    type Output = TripleOutput<T1, T2, T3>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        loop {
            match this.state.take().expect("Cannot poll after ready") {
                TripleState::Idle { op1, op2, op3 } => {
                    match this.runtime.driver.borrow_mut().push_linked_triple(op1, op2, op3) {
                        Ok((key1, key2, key3)) => {
                            *this.state = Some(TripleState::Submitted { key1, key2, key3 });
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }

                TripleState::Submitted { key1, key2, key3 } => {
                    let p1 = this.runtime.poll_task(cx.waker(), key1);
                    let p2 = this.runtime.poll_task(cx.waker(), key2);
                    let p3 = this.runtime.poll_task(cx.waker(), key3);

                    match (p1, p2, p3) {
                        (PushEntry::Ready(r1), PushEntry::Ready(r2), PushEntry::Ready(r3)) => {
                            return Poll::Ready(Ok((r1, r2, r3)));
                        }
                        (PushEntry::Ready(r1), PushEntry::Ready(r2), PushEntry::Pending(k3)) => {
                            *this.state = Some(TripleState::Done12 { r1, r2, key3: k3 });
                        }
                        (PushEntry::Ready(r1), PushEntry::Pending(k2), PushEntry::Ready(r3)) => {
                            *this.state = Some(TripleState::Done13 { r1, key2: k2, r3 });
                        }
                        (PushEntry::Ready(r1), PushEntry::Pending(k2), PushEntry::Pending(k3)) => {
                            *this.state = Some(TripleState::Done1 { r1, key2: k2, key3: k3 });
                        }
                        (PushEntry::Pending(k1), PushEntry::Ready(r2), PushEntry::Ready(r3)) => {
                            *this.state = Some(TripleState::Done23 { key1: k1, r2, r3 });
                        }
                        (PushEntry::Pending(k1), PushEntry::Ready(r2), PushEntry::Pending(k3)) => {
                            *this.state = Some(TripleState::Done2 { key1: k1, r2, key3: k3 });
                        }
                        (PushEntry::Pending(k1), PushEntry::Pending(k2), PushEntry::Ready(r3)) => {
                            *this.state = Some(TripleState::Done3 { key1: k1, key2: k2, r3 });
                        }
                        (PushEntry::Pending(k1), PushEntry::Pending(k2), PushEntry::Pending(k3)) => {
                            *this.state = Some(TripleState::Submitted { key1: k1, key2: k2, key3: k3 });
                        }
                    }
                    return Poll::Pending;
                }

                // --- One done, poll remaining two ---
                TripleState::Done1 { r1, key2, key3 } => {
                    let p2 = this.runtime.poll_task(cx.waker(), key2);
                    let p3 = this.runtime.poll_task(cx.waker(), key3);
                    match (p2, p3) {
                        (PushEntry::Ready(r2), PushEntry::Ready(r3)) => return Poll::Ready(Ok((r1, r2, r3))),
                        (PushEntry::Ready(r2), PushEntry::Pending(k3)) => { *this.state = Some(TripleState::Done12 { r1, r2, key3: k3 }); }
                        (PushEntry::Pending(k2), PushEntry::Ready(r3)) => { *this.state = Some(TripleState::Done13 { r1, key2: k2, r3 }); }
                        (PushEntry::Pending(k2), PushEntry::Pending(k3)) => { *this.state = Some(TripleState::Done1 { r1, key2: k2, key3: k3 }); }
                    }
                    return Poll::Pending;
                }
                TripleState::Done2 { key1, r2, key3 } => {
                    let p1 = this.runtime.poll_task(cx.waker(), key1);
                    let p3 = this.runtime.poll_task(cx.waker(), key3);
                    match (p1, p3) {
                        (PushEntry::Ready(r1), PushEntry::Ready(r3)) => return Poll::Ready(Ok((r1, r2, r3))),
                        (PushEntry::Ready(r1), PushEntry::Pending(k3)) => { *this.state = Some(TripleState::Done12 { r1, r2, key3: k3 }); }
                        (PushEntry::Pending(k1), PushEntry::Ready(r3)) => { *this.state = Some(TripleState::Done23 { key1: k1, r2, r3 }); }
                        (PushEntry::Pending(k1), PushEntry::Pending(k3)) => { *this.state = Some(TripleState::Done2 { key1: k1, r2, key3: k3 }); }
                    }
                    return Poll::Pending;
                }
                TripleState::Done3 { key1, key2, r3 } => {
                    let p1 = this.runtime.poll_task(cx.waker(), key1);
                    let p2 = this.runtime.poll_task(cx.waker(), key2);
                    match (p1, p2) {
                        (PushEntry::Ready(r1), PushEntry::Ready(r2)) => return Poll::Ready(Ok((r1, r2, r3))),
                        (PushEntry::Ready(r1), PushEntry::Pending(k2)) => { *this.state = Some(TripleState::Done13 { r1, key2: k2, r3 }); }
                        (PushEntry::Pending(k1), PushEntry::Ready(r2)) => { *this.state = Some(TripleState::Done23 { key1: k1, r2, r3 }); }
                        (PushEntry::Pending(k1), PushEntry::Pending(k2)) => { *this.state = Some(TripleState::Done3 { key1: k1, key2: k2, r3 }); }
                    }
                    return Poll::Pending;
                }

                // --- Two done, poll remaining one ---
                TripleState::Done12 { r1, r2, key3 } => {
                    match this.runtime.poll_task(cx.waker(), key3) {
                        PushEntry::Ready(r3) => return Poll::Ready(Ok((r1, r2, r3))),
                        PushEntry::Pending(k3) => { *this.state = Some(TripleState::Done12 { r1, r2, key3: k3 }); return Poll::Pending; }
                    }
                }
                TripleState::Done13 { r1, key2, r3 } => {
                    match this.runtime.poll_task(cx.waker(), key2) {
                        PushEntry::Ready(r2) => return Poll::Ready(Ok((r1, r2, r3))),
                        PushEntry::Pending(k2) => { *this.state = Some(TripleState::Done13 { r1, key2: k2, r3 }); return Poll::Pending; }
                    }
                }
                TripleState::Done23 { key1, r2, r3 } => {
                    match this.runtime.poll_task(cx.waker(), key1) {
                        PushEntry::Ready(r1) => return Poll::Ready(Ok((r1, r2, r3))),
                        PushEntry::Pending(k1) => { *this.state = Some(TripleState::Done23 { key1: k1, r2, r3 }); return Poll::Pending; }
                    }
                }
            }
        }
    }
}
