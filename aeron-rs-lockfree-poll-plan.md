# Plan: lock-free `Subscription::poll` — fix UnitedTraders/aeron-rs#35

You are working in a fork of https://github.com/UnitedTraders/aeron-rs (Rust port of the
Aeron client, ~0.1.8, edition 2021). Your job is to implement the fix for upstream issue
**#35** ("Subscription::poll requires a write lock on the hot path — port the lock-free
image list from the C++/Java clients"), validate it thoroughly, and prepare an upstream PR.
Read the issue first: `gh issue view 35 --repo UnitedTraders/aeron-rs`.

The design below was already decided after analysis of the codebase and the issue — don't
relitigate it, but do verify the cited code facts before relying on them (line numbers are
1-based, from master as of 2026-04, commit "Merge pull request #32").

## Context

- `Aeron::find_subscription` / `find_publication` return `Arc<Mutex<Subscription>>` /
  `Arc<Mutex<Publication>>` (`src/aeron.rs`). The background `ClientConductor` thread locks
  the same objects to add/remove images and to close on driver errors. Consumers therefore
  take a mutex on **every** `poll()`/`offer()` — on a busy-spin receive loop that's millions
  of lock/unlock pairs per second and a tail-jitter risk whenever the conductor holds the
  lock (consumer join/leave).
- The reference C++ and Java clients poll lock-free: C++ `Subscription` holds
  `std::atomic<std::shared_ptr<std::vector<Image>>>`; poll is effectively read-only.
- A known downstream consumer (a market-data gateway that busy-spins
  `poll`/`offer` on a pinned thread, consuming this crate via `[patch.crates-io]`) will
  delete its `.lock()` calls once `find_*` returns `Arc<T>` directly.

### Verified code facts (re-verify, then build on them)

- `src/subscription.rs`: `poll(&mut self, ...)` at :209; `round_robin_index: Index` field
  at :35; `image_list: AtomicVec<Image>` at :40; `poll_inner` does `image_list.load_mut()`
  at :165 and bumps `round_robin_index` at :168. `image_count()` (:277) and `block_poll`
  (:248) use `image_list.load()`. Other poll variants: `controlled_poll`,
  `bounded_controlled_poll`, `poll_end_of_streams`, plus `images()` returning `&Vec<Image>`.
- `src/image.rs`: struct at :69 — hot-path state is already interior-mutable or immutable:
  `subscriber_position: UnsafeBufferPosition` (atomic over shared memory),
  `is_closed: Arc<AtomicBool>` ("to make Image cloneable"), `term_buffers`,
  `log_buffers: Arc<LogBuffers>`. `poll(&mut self)` at :308 — the **only** real `&mut` use
  on the hot path is passing `&mut self.header` (a reused per-fragment scratch `Header`) to
  `term_reader::read` at :317. Cold-path mutation: `close(&mut self)` at :706 writes
  `final_position` and `is_eos`. `unsafe impl Send + Sync` at :87-88.
- `src/client_conductor.rs` lock sites on these same objects: publication `close()` on
  driver error/timeout at :1334, :1343 and :1661-:1692; subscription
  `close_and_remove_images` at :1356; `add_image` at :1841; `remove_image` at :1868;
  end-of-stream handling around :1623. The conductor stores `Weak` refs
  (`Arc::downgrade`) in its registration state maps.
- `src/publication.rs`: `offer_part(&self, ...)` at :416 and `close(&self)` at :659 —
  already `&self`. The Mutex around `Publication` guards nothing on the offer path.
- `src/exclusive_publication.rs`: `offer_part(&mut self, ...)` at :431 — inherently
  single-writer (cached term appender state). **Out of scope** (see below).
- `src/concurrent/atomic_vec.rs`: seqlock-ish (`begin_change`/`end_change`), but every
  mutator takes `&mut self`, so its lock-free reader `load(&self)` is unreachable in safe
  code — the external Mutex is currently *load-bearing for soundness*. Do not reuse
  `AtomicVec` in the new design; remove it if nothing else uses it.
- `Header::new(initial_term_id, term_length)` + `set_buffer(...)` — small cursor object,
  rebuilt per fragment by `term_reader::read`; nothing in it needs to survive across poll
  calls (see `src/fragment_assembler.rs` tests for construction).

## Design (decided)

Port the C++ client model, in safe Rust wherever possible:

1. **Image snapshot list**: replace `AtomicVec<Image>` in `Subscription` with
   `arc_swap::ArcSwap<Vec<Image>>` (new dependency `arc-swap`; the issue itself proposes
   it). Readers: `let images = self.image_list.load();` — wait-free, the guard keeps the
   old snapshot alive if the conductor swaps mid-poll. Writer (conductor only): build a new
   `Vec<Image>` by cloning from the current snapshot, apply the add/remove, `store` it.
   Writers are already serialized by the single conductor thread; add a debug assertion or
   a tiny writer-side `Mutex` that the poll path never touches, and say so in a comment.
2. **`Image::poll` family → `&self`** (`poll`, `controlled_poll`, `bounded_poll`,
   `bounded_controlled_poll`, `block_poll`):
   - Kill the persistent `header: Header` field. Keep whatever config it carried
     (`initial_term_id`, term length, etc.) as plain fields on `Image` and construct the
     `Header` **on the stack inside each poll call**. It's a few ints and a buffer pointer;
     this avoids any `UnsafeCell`. Do NOT use `Cell` for anything on `Image` or
     `Subscription` — `Cell` makes the type `!Sync`, which breaks `Arc` sharing with the
     conductor and won't compile.
   - Convert the cold-path mutations for `close(&self)`: `final_position: AtomicI64`,
     `is_eos: AtomicBool`. **This is a correctness requirement, not a style choice**: the
     conductor closes images that a reader's old snapshot may still reference, so
     `close(&mut self)` on a shared image would be aliased mutation.
   - Revisit the `unsafe impl Send + Sync`: after conversion most fields are auto-Send/Sync;
     if `UnsafeBufferPosition`'s raw pointer still forces the unsafe impls, keep them with a
     justifying comment.
3. **`Subscription::poll` → `&self`**: `round_robin_index` becomes an atomic
   (relaxed load/store is fine — see contract below). Convert every list accessor
   (`image_count`, `images`, `poll_end_of_streams`, `block_poll`, ...) to read via the
   snapshot; `images()` can no longer return `&Vec<Image>` — return the snapshot guard or
   an `Arc<Vec<Image>>`.
4. **API change**: `find_subscription` → `Arc<Subscription>`, `find_publication` →
   `Arc<Publication>`. Conductor state maps hold `Weak<Subscription>` / `Weak<Publication>`
   and call the now-`&self` methods directly (no `.lock()`), including `add_image` /
   `remove_image` / `close_and_remove_images`, which become `&self` writer-path methods on
   `Subscription`. Check `Publication` for any remaining `&mut self` methods and convert or
   justify them. Preserve the existing find-protocol semantics: "not ready yet" and
   driver-timeout errors while registration is `Awaiting`, same instance returned on
   repeated `find_*` calls.
5. **Single-poller contract**: with `poll(&self)` on a `Sync` type, two threads *could*
   poll concurrently. Positions are atomics so there is no UB, but fragments may be
   delivered twice. Document "one poller at a time per Subscription/Image" in the doc
   comments — this is exactly the Java/C++ contract; do not try to enforce it at runtime.
6. **Out of scope**: `ExclusivePublication` stays `Arc<Mutex<...>>` (its `offer` is
   inherently `&mut`; a proper fix is an owned-handle + atomic-close-flag design — note it
   in the PR as follow-up work). `FragmentAssembler` wraps handlers, not the subscription;
   verify it compiles unchanged.

## Implementation order

Work in a feature branch (e.g. `lockfree-subscription-poll`), with `upstream` remote set to
UnitedTraders/aeron-rs. Keep commits reviewable:

0. **Baseline first.** Check `.github/workflows/` for how CI runs tests and which media
   driver version it uses. Get the full suite green *before* touching anything:
   `cargo test` (many unit tests need no driver), then build the C media driver for the
   system tests in `tests/` (`aeronmd` from https://github.com/aeron-io/aeron via cmake —
   pick a release whose CnC version equals this crate's `CNC_VERSION = 16`, see
   `src/cnc_file_descriptor.rs:73`; verify against `AERON_CNC_VERSION` in the C sources or
   just run one system test). Record baseline numbers from `examples/throughput.rs` and
   `examples/ping.rs` for the perf comparison.
1. Commit 1 — `Image`: atomics for `final_position`/`is_eos`, `close(&self)`, stack-local
   `Header`, poll family to `&self`. Unit tests in `image.rs` updated.
2. Commit 2 — `Subscription`: `ArcSwap` image list, atomic round-robin, poll family to
   `&self`, writer-path methods for the conductor. Remove `AtomicVec` if now unused.
3. Commit 3 — `ClientConductor` + `Aeron`: `Arc<T>` returns from `find_subscription` /
   `find_publication`, `Weak<T>` state maps, de-`lock()` all conductor call sites. Make
   sure image *lingering* / `on_check_managed_resources` semantics are preserved — with
   `Arc` snapshots the lifetime story may actually simplify, but the timing of
   `LogBuffers` unmapping must not change.
4. Commit 4 — migrate `examples/`, `src/bin/`, `tests/`, doc comments. The diff for users
   should read as: delete `.lock().unwrap()`.
5. Commit 5 — new tests + churn stress test (below), CHANGELOG entry, README migration
   note, version bump to `0.2.0` (breaking change).

## Validation checklist (all must pass)

- `cargo test` and `cargo clippy` clean; match the repo's existing fmt/style (Java-style
  doc comments are intentional — keep them).
- System tests in `tests/` against a running `aeronmd`.
- Examples still work end-to-end against the driver: `basic_publisher` + `basic_subscriber`,
  `ping`, `throughput`. Compare throughput/ping numbers to the step-0 baseline — expect
  equal or better; include before/after in the PR.
- **New churn stress test**: one thread spins `subscription.poll()` continuously while
  publications on the same stream connect and disconnect in a loop (images added/removed by
  the conductor mid-poll), for at least ~30s. Assert: no panic, no lost or duplicated
  fragments (sequence-numbered payloads), image_count converges. This is the test that
  targets the exact race the Mutex used to guard.
- Optional but valuable: run the stress test under ThreadSanitizer
  (`RUSTFLAGS="-Zsanitizer=thread"` on nightly, all deps rebuilt with `-Zbuild-std`).

## PR notes

- Target upstream master, title referencing #35; state that it implements the issue's
  proposal with one deviation to call out explicitly: **stack-local `Header` instead of
  interior mutability**, keeping the port free of new `unsafe`.
- Explain why `AtomicVec` was removed (its lock-free reader was unreachable in safe code
  and unsound if exposed — the wrapping Mutex was load-bearing).
- Include the perf numbers and the churn-test description.
- Mention `ExclusivePublication` as deliberate follow-up scope.
