/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern crate aeron_rs;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use aeron_rs::aeron::Aeron;
use aeron_rs::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
use aeron_rs::concurrent::logbuffer::header::Header;
use aeron_rs::concurrent::strategies::{BusySpinIdleStrategy, Strategy};
use aeron_rs::context::Context;
use aeron_rs::image::Image;
use aeron_rs::utils::errors::AeronError;
use aeron_rs::utils::types::{Index, I64_SIZE};
use lazy_static::lazy_static;

use crate::common::{str_to_c, TEST_CHANNEL, TEST_STREAM_ID};

mod common;

/// How long publications keep connecting and disconnecting while the poller spins.
const CHURN_DURATION: Duration = Duration::from_secs(30);
const MESSAGES_PER_PUBLICATION: i64 = 10_000;
const PAYLOAD_LENGTH: Index = 2 * I64_SIZE; // [publication cycle: i64][sequence number: i64]

lazy_static! {
    static ref POLLER_RUNNING: AtomicBool = AtomicBool::from(true);
    static ref TOTAL_RECEIVED: AtomicI64 = AtomicI64::new(0);
    static ref SEQUENCE_CHECK_FAILED: AtomicBool = AtomicBool::from(false);
    static ref IMAGES_AVAILABLE: AtomicI64 = AtomicI64::new(0);
    static ref IMAGES_UNAVAILABLE: AtomicI64 = AtomicI64::new(0);
}

fn error_handler(error: AeronError) {
    println!("Error: {:?}", error);
}

fn on_available_image(_image: &Image) {
    IMAGES_AVAILABLE.fetch_add(1, Ordering::SeqCst);
}

fn on_unavailable_image(_image: &Image) {
    IMAGES_UNAVAILABLE.fetch_add(1, Ordering::SeqCst);
}

/// One thread spins subscription.poll() while the main thread connects and disconnects
/// sequence-numbered publications on the same stream. The client conductor therefore adds
/// and removes Images from the Subscription in the middle of ongoing polls - exactly the
/// race the Subscription Mutex used to serialize before the image list became a lock-free
/// ArcSwap snapshot. Asserts: no panic, no lost and no duplicated fragments, and the image
/// count converges once the churn stops.
#[test]
fn test_poll_during_image_churn() {
    // Short publication linger so that disconnected publications produce
    // "image unavailable" events (image removals) while the test is still churning.
    let md = common::start_aeron_md_env(&[("AERON_PUBLICATION_LINGER_TIMEOUT", "500ms")]);

    let mut context = Context::new();
    context.set_error_handler(Box::new(error_handler));
    context.set_available_image_handler(Box::new(on_available_image));
    context.set_unavailable_image_handler(Box::new(on_unavailable_image));
    context.set_pre_touch_mapped_memory(true);

    let mut aeron = Aeron::new(context).expect("Error creating Aeron instance");

    let subscription_id = aeron
        .add_subscription(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
        .expect("Error adding subscription");

    let subscription = loop {
        if let Ok(subscription) = aeron.find_subscription(subscription_id) {
            break subscription;
        }
        thread::sleep(Duration::from_millis(10));
    };

    let poller_subscription = subscription.clone();
    let poller_thread = thread::Builder::new()
        .name(String::from("churn poller"))
        .spawn(move || {
            // Sequence numbers are tracked per publication cycle: every cycle publishes
            // a fresh session (Image), and fragments within a session arrive in order.
            let mut last_sequence_numbers: HashMap<i64, i64> = HashMap::new();

            let mut handler = |buffer: &AtomicBuffer, offset: Index, length: Index, _header: &Header| {
                assert_eq!(length, PAYLOAD_LENGTH);
                let cycle = buffer.get::<i64>(offset);
                let seq_no = buffer.get::<i64>(offset + I64_SIZE);

                let last = last_sequence_numbers.entry(cycle).or_insert(-1);
                if seq_no != *last + 1 {
                    // A gap means a lost fragment, seq_no <= last means a duplicated one.
                    SEQUENCE_CHECK_FAILED.store(true, Ordering::SeqCst);
                    println!("SEQUENCE CHECK FAILED: cycle {} got {} after {}", cycle, seq_no, *last);
                }
                *last = seq_no;

                TOTAL_RECEIVED.fetch_add(1, Ordering::SeqCst);
            };

            let poll_idle_strategy = BusySpinIdleStrategy::default();

            while POLLER_RUNNING.load(Ordering::SeqCst) {
                let fragments_read = poller_subscription.poll(&mut handler, 20);
                poll_idle_strategy.idle_opt(fragments_read);
            }
        })
        .expect("Can't start poller thread");

    let buffer = AlignedBuffer::with_capacity(PAYLOAD_LENGTH);
    let src_buffer = AtomicBuffer::from_aligned(&buffer);

    let start = Instant::now();
    let mut cycle: i64 = 0;
    let mut total_sent: i64 = 0;

    while start.elapsed() < CHURN_DURATION {
        let publication_id = aeron
            .add_publication(str_to_c(TEST_CHANNEL), TEST_STREAM_ID)
            .expect("Error adding publication");

        let publication = loop {
            match aeron.find_publication(publication_id) {
                Ok(publication) => break publication,
                Err(_) => thread::sleep(Duration::from_millis(1)),
            }
        };

        for seq_no in 0..MESSAGES_PER_PUBLICATION {
            src_buffer.put::<i64>(0, cycle);
            src_buffer.put::<i64>(I64_SIZE, seq_no);

            // Retry not-yet-connected / back-pressured / admin-action outcomes.
            while publication.offer(src_buffer).is_err() {
                thread::yield_now();
            }
        }
        total_sent += MESSAGES_PER_PUBLICATION;

        // Wait until the poller has consumed everything this publication sent,
        // then disconnect it (the drop releases the publication in the driver).
        let receive_deadline = Instant::now() + Duration::from_secs(10);
        while TOTAL_RECEIVED.load(Ordering::SeqCst) < total_sent {
            assert!(
                Instant::now() < receive_deadline,
                "cycle {}: poller received {} of {} sent messages",
                cycle,
                TOTAL_RECEIVED.load(Ordering::SeqCst),
                total_sent
            );
            thread::yield_now();
        }

        drop(publication);
        cycle += 1;
    }

    println!(
        "Churned {} publications in {:?}: {} messages sent and received",
        cycle,
        start.elapsed(),
        total_sent
    );
    assert!(cycle >= 2, "expected at least two connect/disconnect cycles");
    assert!(!SEQUENCE_CHECK_FAILED.load(Ordering::SeqCst));
    assert_eq!(TOTAL_RECEIVED.load(Ordering::SeqCst), total_sent);

    // Every cycle created one Image; with the 500ms publication linger their removal
    // happened while later cycles were still being polled.
    assert_eq!(IMAGES_AVAILABLE.load(Ordering::SeqCst), cycle);
    assert!(IMAGES_UNAVAILABLE.load(Ordering::SeqCst) > 0);

    // After the churn stops the image count must converge to zero: all images get
    // removed and every removal is observed by the still-spinning poller.
    let converge_deadline = Instant::now() + Duration::from_secs(30);
    while subscription.image_count() > 0 {
        assert!(
            Instant::now() < converge_deadline,
            "image_count did not converge: {} images left",
            subscription.image_count()
        );
        thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(IMAGES_UNAVAILABLE.load(Ordering::SeqCst), cycle);

    POLLER_RUNNING.store(false, Ordering::SeqCst);
    poller_thread.join().expect("poller thread panicked");

    common::stop_aeron_md(md);
}
