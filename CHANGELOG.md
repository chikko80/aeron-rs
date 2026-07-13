# Changelog

## 0.2.0

### Breaking: lock-free `Subscription::poll` ([#35](https://github.com/UnitedTraders/aeron-rs/issues/35))

`Aeron::find_subscription` now returns `Arc<Subscription>` and
`Aeron::find_publication` returns `Arc<Publication>` (previously
`Arc<Mutex<...>>`). Polling and offering no longer take any lock: the
`Subscription` image list is a copy-on-write atomic snapshot
(`arc_swap::ArcSwap<Vec<Image>>`, the model of the C++/Java clients), the whole
`Image`/`Subscription` poll family and `Publication::try_claim`/`offer_bulk`
take `&self`, and the client conductor adds/removes images without blocking an
in-flight poll.

Migration: delete `.lock().unwrap()` at your `Subscription`/`Publication` call
sites; everything else keeps working as before.

```rust
// 0.1.x
let fragments = subscription.lock().unwrap().poll(&mut handler, 10);
// 0.2.x
let fragments = subscription.poll(&mut handler, 10);
```

As in the Java and C++ clients, a `Subscription`/`Image` is meant to be polled
by one thread at a time. Concurrent polling is memory safe (positions are
atomic) but fragments may be delivered to more than one poller.

Other changes in this release:

- `Subscription::images()` returns an immutable `Arc<Vec<Image>>` snapshot
  instead of `&Vec<Image>`; `image_by_session_id`/`image_by_index` return owned
  `Image` copies (which share their underlying log buffer and position).
- `Image::close`, `Subscription::add_image`/`remove_image`/
  `close_and_remove_images` take `&self` (conductor-only writer paths).
- Removed `concurrent::atomic_vec::AtomicVec`: all its mutators took
  `&mut self`, so its lock-free reader was unreachable in safe code, and it
  would have been unsound (a data race) if exposed under real concurrency -
  the `Mutex` around `Subscription` was load-bearing for its soundness.
- `ExclusivePublication` intentionally still returned as
  `Arc<Mutex<ExclusivePublication>>`: its offer path is inherently
  single-writer (`&mut self`, cached term-appender state). An owned-handle
  design for it is planned follow-up work.
