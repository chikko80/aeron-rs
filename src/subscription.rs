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

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use crate::client_conductor::ClientConductor;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::header::Header;
use crate::concurrent::logbuffer::term_scan::BlockHandler;
use crate::concurrent::status::status_indicator_reader;
use crate::image::{ControlledPollAction, Image};
use crate::utils::errors::{AeronError, GenericError, IllegalStateError};
use crate::utils::types::Index;

pub struct Subscription {
    conductor: Arc<Mutex<ClientConductor>>,
    channel: CString,
    channel_status_id: i32,
    round_robin_index: AtomicUsize,
    //todo std::size_t
    registration_id: i64,
    stream_id: i32,

    /// Copy-on-write snapshot of the Images attached to this Subscription, following the
    /// C++ client model (std::atomic<std::shared_ptr<std::vector<Image>>>). The poll path
    /// loads the snapshot wait-free; the guard keeps an old snapshot alive if the client
    /// conductor swaps a new one in mid-poll.
    image_list: ArcSwap<Vec<Image>>,
    /// Serializes image-list writers. In practice all writers run on the single client
    /// conductor thread, so this lock is uncontended; the poll path never touches it.
    image_list_writer_lock: Mutex<()>,
    is_closed: AtomicBool,
}

impl Subscription {
    pub fn new(
        conductor: Arc<Mutex<ClientConductor>>,
        registration_id: i64,
        channel: CString,
        stream_id: i32,
        channel_status_id: i32,
    ) -> Self {
        Self {
            conductor,
            channel,
            channel_status_id,
            round_robin_index: AtomicUsize::new(0),
            registration_id,
            stream_id,
            image_list: ArcSwap::from_pointee(vec![]),
            image_list_writer_lock: Mutex::new(()),
            is_closed: AtomicBool::from(false),
        }
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    pub fn channel(&self) -> CString {
        self.channel.clone()
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    pub fn stream_id(&self) -> i32 {
        self.stream_id
    }

    /**
     * Registration Id returned by Aeron::addSubscription when this Subscription was added.
     *
     * @return the registrationId of the subscription.
     */
    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @return the counter id used to represent the channel status.
     */
    pub fn channel_status_id(&self) -> i32 {
        self.channel_status_id
    }

    pub fn add_destination(&self, endpoint_channel: String) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(IllegalStateError::SubscriptionClosed.into());
        }

        if let Ok(endpoint_channel_cstr) = CString::new(endpoint_channel) {
            self.conductor
                .lock()
                .expect("Mutex poisoned")
                .add_rcv_destination(self.registration_id, endpoint_channel_cstr)
        } else {
            Err(GenericError::StringToCStringConversionFailed.into())
        }
    }

    pub fn remove_destination(&self, endpoint_channel: String) -> Result<i64, AeronError> {
        if self.is_closed() {
            return Err(IllegalStateError::SubscriptionClosed.into());
        }

        if let Ok(endpoint_channel_cstr) = CString::new(endpoint_channel) {
            self.conductor
                .lock()
                .expect("Mutex poisoned")
                .remove_rcv_destination(self.registration_id, endpoint_channel_cstr)
        } else {
            Err(GenericError::StringToCStringConversionFailed.into())
        }
    }

    pub fn find_destination_response(&self, correlation_id: i64) -> Result<bool, AeronError> {
        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .find_destination_response(correlation_id)
    }

    pub fn channel_status(&self) -> i64 {
        if self.is_closed() {
            return status_indicator_reader::NO_ID_ALLOCATED as i64;
        }

        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .channel_status(self.channel_status_id)
    }

    /**
     * Poll the Image s under the subscription for having reached End of Stream.
     *
     * @param end_of_stream_handler callback for handling end of stream indication.
     * @return number of Image s that have reached End of Stream.
     * @deprecated
     */
    pub fn poll_end_of_streams(&self, end_of_stream_handler: EndOfStreamHandler) -> i32 {
        self.image_list
            .load()
            .iter()
            .filter(|image| image.is_end_of_stream())
            .inspect(|image| end_of_stream_handler(image))
            .count() as _
    }

    fn poll_inner(&self, fragment_limit: i32, mut poll_kind: impl FnMut(&Image, i32) -> i32) -> i32 {
        let image_list = self.image_list.load();
        let length = image_list.len();

        let mut starting_index = self.round_robin_index.load(Ordering::Relaxed);

        if starting_index >= length {
            starting_index = 0;
            self.round_robin_index.store(0, Ordering::Relaxed);
        } else {
            self.round_robin_index.store(starting_index + 1, Ordering::Relaxed);
        }

        let mut fragments_read = 0;
        for i in starting_index..length {
            if fragments_read < fragment_limit {
                fragments_read += poll_kind(&image_list[i], fragment_limit - fragments_read);
            }
        }

        for i in 0..starting_index {
            if fragments_read < fragment_limit {
                fragments_read += poll_kind(&image_list[i], fragment_limit - fragments_read);
            }
        }

        fragments_read
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     * <p>
     * This method is lock-free: the client conductor may add and remove {@link Image}s concurrently with the
     * poll without blocking it. As in the C++ and Java clients a Subscription is meant to be polled by one
     * thread at a time; polling from several threads concurrently is memory safe but fragments may then be
     * delivered to more than one of the pollers.
     *
     * @param fragment_handler callback for handling each message fragment as it is read.
     * @param fragment_limit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received
     *
     * @see fragment_handler_t
     */
    pub fn poll(&self, fragment_handler: &mut impl FnMut(&AtomicBuffer, Index, Index, &Header), fragment_limit: i32) -> i32 {
        self.poll_inner(fragment_limit, |image, fragments_left| {
            image.poll(fragment_handler, fragments_left)
        })
    }

    /**
     * Poll in a controlled manner the Image s under the subscription for available message fragments.
     * Control is applied to fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use controlled_poll_fragment_handler_t.
     *
     * @param fragment_handler callback for handling each message fragment as it is read.
     * @param fragment_limit   number of message fragments to limit for the poll operation across multiple Image s.
     * @return the number of fragments received
     * @see controlled_poll_fragment_handler_t
     */
    pub fn controlled_poll(
        &self,
        mut fragment_handler: impl FnMut(&AtomicBuffer, Index, Index, &Header) -> Result<ControlledPollAction, AeronError>,
        fragment_limit: i32,
    ) -> i32 {
        self.poll_inner(fragment_limit, |image, fragments_left| {
            image.controlled_poll(&mut fragment_handler, fragments_left)
        })
    }

    /**
     * Poll the Image s under the subscription for available message fragments in blocks.
     *
     * @param block_handler     to receive a block of fragments from each Image.
     * @param block_length_limit for each individual block.
     * @return the number of bytes consumed.
     */
    pub fn block_poll(&self, block_handler: BlockHandler, block_length_limit: i32) -> i64 {
        let image_list = self.image_list.load();

        let mut bytes_consumed: i64 = 0;

        for image in image_list.iter() {
            bytes_consumed += image.block_poll(block_handler, block_length_limit) as i64;
        }

        bytes_consumed
    }

    /**
     * Is the subscription connected by having at least one open image available.
     *
     * @return true if the subscription has more than one open image available.
     */
    pub fn is_connected(&self) -> bool {
        self.image_list.load().iter().any(|image| !image.is_closed())
    }

    /**
     * Count of images associated with this subscription.
     *
     * @return count of images associated with this subscription.
     */
    pub fn image_count(&self) -> usize {
        self.image_list.load().len()
    }

    /**
     * Return the {@link Image} associated with the given session_id.
     *
     * This method returns a copy of the Image overlaying the logbuffer.
     * It is up to the application to not use the Image if it becomes unavailable.
     *
     * @param session_id associated with the Image.
     * @return Image associated with the given session_id or None if no Image exists.
     */
    pub fn image_by_session_id(&self, session_id: i32) -> Option<Image> {
        self.image_list
            .load()
            .iter()
            .find(|img| img.session_id() == session_id)
            .cloned()
    }

    /**
     * Get the image at the given index from the images array.
     *
     * This method returns a copy of the Image overlaying the logbuffer.
     * It is up to the application to not use the Image if it becomes unavailable.
     *
     * @param index in the array
     * @return image at given index or None if out of range.
     */
    pub fn image_by_index(&self, index: usize) -> Option<Image> {
        self.image_list.load().get(index).cloned()
    }

    /**
     * Get a snapshot of the active {@link Image}s that match this subscription.
     *
     * The returned snapshot is immutable: Images added or removed afterwards do not
     * show up in it.
     *
     * @return a snapshot of active {@link Image}s that match this subscription.
     */
    pub fn images(&self) -> Arc<Vec<Image>> {
        self.image_list.load_full()
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    pub fn has_image(&self, correlation_id: i64) -> bool {
        self.image_list
            .load()
            .iter()
            .any(|img| img.correlation_id() == correlation_id)
    }

    /// Adds image to the subscription and returns Images
    /// as they were just before adding this Image.
    ///
    /// Meant to be called from the client conductor thread only.
    pub fn add_image(&self, image: Image) -> Vec<Image> {
        let _guard = self.image_list_writer_lock.lock().expect("Mutex poisoned");

        let old_image_list = self.image_list.load_full();
        let mut new_image_list = (*old_image_list).clone();
        new_image_list.push(image);
        self.image_list.store(Arc::new(new_image_list));

        Arc::try_unwrap(old_image_list).unwrap_or_else(|images| (*images).clone())
    }

    /// Removes image with given correlation_id and returns old Images (as of before removal)
    /// and index of removed element.
    /// Returns None if Image was not removed (e.g. was not found).
    ///
    /// Meant to be called from the client conductor thread only.
    pub fn remove_image(&self, correlation_id: i64) -> Option<(Vec<Image>, Index)> {
        let _guard = self.image_list_writer_lock.lock().expect("Mutex poisoned");

        let old_image_list = self.image_list.load_full();

        let index = old_image_list.iter().position(|image| {
            if image.correlation_id() == correlation_id {
                // The close state is shared between Image clones, therefore pollers
                // still holding this Image in an older snapshot observe the close too.
                image.close();
                true
            } else {
                false
            }
        })?;

        let mut new_image_list = (*old_image_list).clone();
        new_image_list.remove(index);
        self.image_list.store(Arc::new(new_image_list));

        let old_images = Arc::try_unwrap(old_image_list).unwrap_or_else(|images| (*images).clone());
        Some((old_images, index as Index))
    }

    /// Removes all images and returns old Images if subscription is not closed.
    /// Returns None if subscription is closed.
    ///
    /// Meant to be called from the client conductor thread only.
    pub fn close_and_remove_images(&self) -> Option<Vec<Image>> {
        if !self.is_closed.swap(true, Ordering::SeqCst) {
            let _guard = self.image_list_writer_lock.lock().expect("Mutex poisoned");
            let old_image_list = self.image_list.swap(Arc::new(vec![]));
            Some(Arc::try_unwrap(old_image_list).unwrap_or_else(|images| (*images).clone()))
        } else {
            None
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let old_image_list = self.image_list.swap(Arc::new(vec![]));
        let list = Arc::try_unwrap(old_image_list).unwrap_or_else(|images| (*images).clone());

        self.conductor
            .lock()
            .expect("Mutex poisoned")
            .release_subscription(self.registration_id, list)
            .ok();
    }
}

type EndOfStreamHandler = fn(&Image);
