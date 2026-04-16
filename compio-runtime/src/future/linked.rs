//! Future for submitting linked operation pairs to the runtime.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use compio_buf::BufResult;
use compio_driver::{Key, OpCode, PushEntry};

use crate::Runtime;

pin_project_lite::pin_project! {
    /// Future returned by [`Runtime::submit_linked_pair`].
    ///
    /// Submits two operations as an io_uring linked chain. The kernel
    /// executes the second operation only after the first completes
    /// successfully. If the first fails, the second is cancelled
    /// (`-ECANCELED`).
    ///
    /// Resolves when **both** operations have completed. Returns
    /// `Err` if the linked submission itself fails (e.g. unsupported
    /// opcode for linking).
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
    /// Both ops are ready to be submitted.
    Idle { op1: T1, op2: T2 },
    /// Both ops are submitted and in-flight.
    Submitted { key1: Key<T1>, key2: Key<T2> },
    /// First op completed, waiting for second.
    FirstDone {
        result1: BufResult<usize, T1>,
        key2: Key<T2>,
    },
    /// Second op completed, waiting for first.
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
