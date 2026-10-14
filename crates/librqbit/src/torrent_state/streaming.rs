use std::{
    collections::{HashSet, VecDeque},
    io::SeekFrom,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Instant,
};

use anyhow::Context;
use dashmap::DashMap;
use parking_lot::RwLock;

use librqbit_core::lengths::{CurrentPiece, Lengths, ValidPieceIndex};
use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::{Notify, OwnedSemaphorePermit},
};
use tracing::{debug, trace};

use crate::{ManagedTorrent, file_info::FileInfo, storage::TorrentStorage};

use super::{ManagedTorrentHandle, TorrentMetadata};

type StreamId = usize;

// 32 mb lookahead by default.
const PER_STREAM_BUF_DEFAULT: u64 = 32 * 1024 * 1024;

struct StreamState {
    file_id: usize,
    file_len: u64,
    file_abs_offset: u64,
    position: u64,
    waker: Option<Waker>,
}

impl StreamState {
    fn current_piece(&self, lengths: &Lengths) -> Option<CurrentPiece> {
        lengths.compute_current_piece(self.position, self.file_abs_offset)
    }

    fn queue<'a>(&self, lengths: &'a Lengths) -> impl Iterator<Item = ValidPieceIndex> + use<'a> {
        let start = self.file_abs_offset + self.position;
        let end = (start + PER_STREAM_BUF_DEFAULT).min(self.file_abs_offset + self.file_len);
        let dpl = lengths.default_piece_length();
        let start_id = (start / dpl as u64).try_into().unwrap();
        let end_id = end.div_ceil(dpl as u64).try_into().unwrap();
        (start_id..end_id).filter_map(|i| lengths.validate_piece_index(i))
    }
}

pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    streams: DashMap<StreamId, StreamState>,
    requested_pieces: RwLock<Option<Arc<[ValidPieceIndex]>>>,
    pub(crate) new_pieces_notify: Notify,
    background_completion: AtomicBool,
}

impl Default for TorrentStreams {
    fn default() -> Self {
        Self {
            next_stream_id: AtomicUsize::new(0),
            streams: DashMap::new(),
            requested_pieces: RwLock::new(None),
            new_pieces_notify: Notify::new(),
            // Ordinary rqbit downloads retain upstream's completion behavior.
            background_completion: AtomicBool::new(true),
        }
    }
}

impl TorrentStreams {
    fn next_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_waker(&self, stream_id: StreamId, waker: Waker) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let vm = s.value_mut();
            vm.waker = Some(waker);
        }
    }

    /// Execute the caller's queue, or use upstream reader scheduling when unset.
    pub(crate) fn iter_next_pieces<'a>(
        &'a self,
        lengths: &'a Lengths,
    ) -> impl Iterator<Item = ValidPieceIndex> + 'a {
        if let Some(pieces) = self.requested_pieces.read().clone() {
            return itertools::Either::Left((0..pieces.len()).map(move |i| pieces[i]));
        }
        struct Interleave<I> {
            all: VecDeque<I>,
        }

        impl<I: Iterator<Item = ValidPieceIndex>> Iterator for Interleave<I> {
            type Item = ValidPieceIndex;

            fn next(&mut self) -> Option<Self::Item> {
                while let Some(mut it) = self.all.pop_front() {
                    if let Some(piece) = it.next() {
                        self.all.push_back(it);
                        return Some(piece);
                    }
                }
                None
            }
        }

        let mut all: Vec<_> = self.streams.iter().map(|s| s.queue(lengths)).collect();

        // Shuffle to decrease determinism and make queueing fairer.
        use rand::seq::SliceRandom;
        all.shuffle(&mut rand::rng());

        itertools::Either::Right(Interleave { all: all.into() })
    }

    /// Ordering, deduplication and consumer lifetimes belong to the caller.
    fn set_piece_request_queue(&self, pieces: Option<Vec<ValidPieceIndex>>) -> bool {
        let mut current = self.requested_pieces.write();
        if current.as_deref() == pieces.as_deref() {
            return false;
        }
        let reconnect = pieces.as_ref().is_none_or(|pieces| {
            let previous: HashSet<_> = current
                .as_deref()
                .unwrap_or_default()
                .iter()
                .copied()
                .collect();
            pieces.iter().any(|piece| !previous.contains(piece))
        });
        *current = pieces.map(Arc::from);
        drop(current);
        self.new_pieces_notify.notify_waiters();
        reconnect
    }
    pub(crate) fn is_piece_requested(&self, piece: ValidPieceIndex) -> bool {
        self.requested_pieces
            .read()
            .as_deref()
            .is_some_and(|pieces| pieces.contains(&piece))
    }
    pub(crate) fn background_completion_enabled(&self) -> bool {
        self.background_completion.load(Ordering::Relaxed)
    }

    pub(crate) fn wake_streams_on_piece_completed(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) {
        for mut w in self.streams.iter_mut() {
            if w.value().current_piece(lengths).map(|p| p.id) == Some(piece_id)
                && let Some(waker) = w.value_mut().waker.take()
            {
                debug!(
                    stream_id = *w.key(),
                    piece_id = piece_id.get(),
                    "waking stream"
                );
                waker.wake();
            }
        }
    }

    fn drop_stream(&self, stream_id: StreamId) -> Option<StreamState> {
        debug!(stream_id, "dropping stream");
        let removed = self.streams.remove(&stream_id).map(|s| s.1);
        if removed.is_some() {
            self.new_pieces_notify.notify_waiters();
        }
        removed
    }

    pub(crate) fn streamed_file_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.streams.iter().map(|s| s.value().file_id)
    }
}

pub struct FileStream {
    torrent: ManagedTorrentHandle,
    metadata: Arc<TorrentMetadata>,
    streams: Arc<TorrentStreams>,
    stream_id: usize,
    file_id: usize,
    position: u64,

    // file params
    file_len: u64,
    file_torrent_abs_offset: u64,

    _blocking_permit: OwnedSemaphorePermit,
}

macro_rules! map_io_err {
    ($e:expr) => {
        $e.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    };
}

macro_rules! poll_try_io {
    ($e:expr) => {{
        let e = map_io_err!($e);
        match e {
            Ok(r) => r,
            Err(e) => {
                debug!("stream error {e:#}");
                return Poll::Ready(Err(e));
            }
        }
    }};
}

impl AsyncRead for FileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // if the file is over, return 0
        if self.position == self.file_len {
            debug!(
                stream_id = self.stream_id,
                file_id = self.file_id,
                "stream completed, EOF"
            );
            return Poll::Ready(Ok(()));
        }

        let current = poll_try_io!(
            self.metadata
                .lengths()
                .compute_current_piece(self.position, self.file_torrent_abs_offset)
                .context("invalid position")
        );

        // if the piece is not there, register to wake when it is
        // check if we have the piece for real
        let have = poll_try_io!(self.torrent.with_chunk_tracker(|ct| {
            let have = ct.get_have_pieces().as_slice()[current.id.get() as usize];
            if !have {
                self.streams
                    .register_waker(self.stream_id, cx.waker().clone());
            }
            have
        }));
        if !have {
            debug!(stream_id = self.stream_id, file_id = self.file_id, piece_id = %current.id, "poll pending, not have");
            return Poll::Pending;
        }

        // actually stream the piece
        let buf = tbuf.initialize_unfilled();
        let file_remaining = self.file_len - self.position;
        let bytes_to_read: usize = poll_try_io!(
            (buf.len() as u64)
                .min(current.piece_remaining as u64)
                .min(file_remaining)
                .try_into()
        );

        let buf = &mut buf[..bytes_to_read];

        let start = Instant::now();
        poll_try_io!(poll_try_io!(self.torrent.shared.spawner.block_in_place(
            || {
                self.torrent.with_storage_and_file(
                    self.file_id,
                    |files, _fi| {
                        files.pread_exact(self.file_id, self.position, buf)?;
                        Ok::<_, anyhow::Error>(())
                    },
                    &self.metadata,
                )
            }
        )));

        trace!(
            buflen = buf.len(),
            stream_id = self.stream_id,
            file_id = self.file_id,
            read_time = ?start.elapsed(),
            "will write bytes"
        );

        self.as_mut().advance(bytes_to_read as u64);
        tbuf.advance(bytes_to_read);

        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let end_i64 = map_io_err!(TryInto::<i64>::try_into(self.file_len))?;
        let new_pos: i64 = match position {
            SeekFrom::Start(s) => map_io_err!(s.try_into())?,
            SeekFrom::End(e) => map_io_err!(TryInto::<i64>::try_into(self.file_len))? + e,
            SeekFrom::Current(o) => map_io_err!(TryInto::<i64>::try_into(self.position))? + o,
        };

        if new_pos < 0 || new_pos > end_i64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                anyhow::anyhow!("invalid seek"),
            ));
        }

        self.as_mut().set_position(map_io_err!(new_pos.try_into())?);
        debug!(stream_id = self.stream_id, position = self.position, "seek");
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        self.streams.drop_stream(self.stream_id);
    }
}

impl ManagedTorrent {
    fn with_storage_and_file<F, R>(
        &self,
        file_id: usize,
        f: F,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<R>
    where
        F: FnOnce(&dyn TorrentStorage, &FileInfo) -> R,
    {
        self.with_state(|s| {
            let files = match s {
                crate::ManagedTorrentState::Paused(p) => &*p.files,
                crate::ManagedTorrentState::Live(l) => &*l.files,
                s => anyhow::bail!("with_storage_and_file: invalid state: {}", s.name()),
            };
            let fi = metadata.file_infos.get(file_id).context("invalid file")?;
            Ok(f(files, fi))
        })
    }

    fn streams(&self) -> anyhow::Result<Arc<TorrentStreams>> {
        self.with_state(|s| match s {
            crate::ManagedTorrentState::Paused(p) => Ok(p.streams.clone()),
            crate::ManagedTorrentState::Live(l) => Ok(l.streams.clone()),
            s => anyhow::bail!("streams: invalid state {}", s.name()),
        })
    }

    /// Replace the torrent's ordered piece request queue.
    ///
    /// `Some(pieces)` overrides automatic reader lookahead. `Some(Vec::new())`
    /// supplies no priority pieces; `None` restores automatic reader lookahead.
    /// Selected files still complete unless background completion is disabled
    /// with [`Self::set_background_completion`].
    ///
    /// Queue order guides piece acquisition, subject to peer availability and
    /// pieces already in flight; it does not guarantee completion order.
    /// Replacing the queue does not cancel requests already sent to peers.
    /// There is one queue per torrent. Combining consumer requests and removing
    /// duplicates belong to the caller. Indices are checked against this
    /// torrent before replacing the queue; an invalid queue leaves the current
    /// queue intact. The queue can be replaced while the torrent is live or paused.
    /// Pause/resume retains the queue. Concurrent setters replace the same queue;
    /// the caller coordinates ownership. Dropping a reader does not reset it.
    /// To control demand before any payload is requested, add the torrent paused,
    /// wait for initialization, configure both controls, and then unpause it.
    /// The two setters are independent operations, not an atomic mode switch.
    ///
    /// # Example
    ///
    /// Request the last and first pieces, then let the application read the
    /// corresponding byte ranges. Restore normal scheduling when it is done.
    ///
    /// ```no_run
    /// use librqbit::ManagedTorrent;
    ///
    /// async fn request_ends(handle: &ManagedTorrent) -> anyhow::Result<()> {
    ///     handle.wait_until_initialized().await?;
    ///     let pieces = handle.with_metadata(|metadata| {
    ///         let lengths = metadata.lengths();
    ///         let mut pieces = vec![lengths.last_piece_id()];
    ///         if lengths.total_pieces() > 1 {
    ///             pieces.push(lengths.validate_piece_index(0).unwrap());
    ///         }
    ///         pieces
    ///     })?;
    ///     handle.set_background_completion(false)?;
    ///     handle.set_piece_request_queue(Some(pieces))
    /// }
    ///
    /// fn restore_downloads(handle: &ManagedTorrent) -> anyhow::Result<()> {
    ///     handle.set_piece_request_queue(None)?;
    ///     handle.set_background_completion(true)
    /// }
    /// ```
    pub fn set_piece_request_queue(
        &self,
        pieces: Option<Vec<ValidPieceIndex>>,
    ) -> anyhow::Result<()> {
        let streams = self.streams()?;
        if let Some(pieces) = pieces.as_ref() {
            self.with_metadata(|metadata| {
                if let Some(piece) = pieces.iter().find(|piece| {
                    metadata
                        .lengths()
                        .validate_piece_index(piece.get())
                        .is_none()
                }) {
                    anyhow::bail!("piece {} is outside this torrent", piece.get());
                }
                Ok(())
            })??;
        }
        if streams.set_piece_request_queue(pieces) {
            self.with_state(|state| {
                if let crate::ManagedTorrentState::Live(l) = state {
                    l.reconnect_all_not_needed_peers();
                }
            });
        }
        Ok(())
    }

    /// Enable or disable completion of the remaining selected-file pieces.
    ///
    /// Enabled by default. When disabled, new piece acquisition uses only the
    /// explicit request queue, or automatic reader lookahead if no queue is set.
    /// When enabled, selected-file scheduling provides the fallback after those
    /// priority pieces. This does not pause the torrent or cancel peer requests
    /// already in flight. With an explicit empty queue and completion disabled,
    /// no additional missing pieces are selected.
    pub fn set_background_completion(&self, enabled: bool) -> anyhow::Result<()> {
        let streams = self.streams()?;
        if streams
            .background_completion
            .swap(enabled, Ordering::Relaxed)
            != enabled
        {
            streams.new_pieces_notify.notify_waiters();
            if enabled {
                self.with_state(|state| {
                    if let crate::ManagedTorrentState::Live(l) = state {
                        l.reconnect_all_not_needed_peers();
                    }
                });
            }
        }
        Ok(())
    }

    fn maybe_reconnect_needed_peers_for_file(&self, file_id: usize) -> bool {
        // If we have the full file, don't bother.
        if self.is_file_finished(file_id) {
            return false;
        }
        self.with_state(|state| {
            if let crate::ManagedTorrentState::Live(l) = &state {
                l.reconnect_all_not_needed_peers();
            }
        });
        true
    }

    fn is_file_finished(&self, file_id: usize) -> bool {
        let metadata = self.metadata.load();
        let metadata = match metadata.as_ref() {
            Some(r) => r,
            None => return false,
        };
        // TODO: would be nice to remove locking
        self.with_chunk_tracker(|ct| ct.is_file_finished(&metadata.file_infos[file_id]))
            .unwrap_or(false)
    }

    pub async fn stream(self: Arc<Self>, file_id: usize) -> anyhow::Result<FileStream> {
        // Streams need live or paused state, so wait out the initial check
        // of existing data instead of failing while it runs.
        self.wait_until_initialized().await?;
        let metadata = self
            .metadata
            .load_full()
            .context("torrent metadata is not resolved")?;
        let (fd_len, fd_offset) = self.with_storage_and_file(
            file_id,
            |_fd, fi| (fi.len, fi.offset_in_torrent),
            &metadata,
        )?;
        let streams = self.streams()?;
        let blocking_permit = self.shared().spawner.semaphore().acquire_owned().await?;
        let s = FileStream {
            stream_id: streams.next_id(),
            streams: streams.clone(),
            file_id,
            position: 0,

            file_len: fd_len,
            file_torrent_abs_offset: fd_offset,
            _blocking_permit: blocking_permit,
            torrent: self,
            metadata,
        };
        s.torrent.maybe_reconnect_needed_peers_for_file(file_id);
        streams.streams.insert(
            s.stream_id,
            StreamState {
                file_id,
                position: 0,
                waker: None,
                file_len: fd_len,
                file_abs_offset: fd_offset,
            },
        );

        streams.new_pieces_notify.notify_waiters();
        debug!(stream_id = s.stream_id, file_id, "started stream");

        Ok(s)
    }
}

impl FileStream {
    pub fn position(&self) -> u64 {
        self.position
    }

    fn advance(&mut self, diff: u64) {
        self.set_position(self.position + diff)
    }

    fn set_position(&mut self, new_pos: u64) {
        self.position = new_pos;
        let changed = {
            let mut stream = self.streams.streams.get_mut(&self.stream_id).unwrap();
            let changed =
                stream.position != new_pos && self.streams.requested_pieces.read().is_none();
            stream.position = new_pos;
            changed
        };
        if changed {
            self.streams.new_pieces_notify.notify_waiters();
        }
    }

    pub fn len(&self) -> u64 {
        self.file_len
    }
}

#[cfg(test)]
mod demand_tests {
    use super::*;

    #[test]
    #[ignore = "CPU-only queue benchmark; run in release mode with --ignored --nocapture"]
    fn piece_queue_update_benchmark() {
        use std::hint::black_box;
        let lengths = Lengths::new(16_384 * 32_768, 32_768).unwrap();
        let measure = |label: &str, size, iterations, mut operation: Box<dyn FnMut()>| {
            let start = Instant::now();
            for _ in 0..iterations {
                operation();
            }
            println!(
                "queue-bench {label} size={size} iterations={iterations} ns/op={}",
                start.elapsed().as_nanos() / iterations as u128
            );
        };
        let streams = Arc::new(TorrentStreams::default());
        measure(
            "unset",
            0,
            100_000,
            Box::new(move || {
                black_box(streams.iter_next_pieces(&lengths).count());
            }),
        );
        for size in [16, 128, 1024, 8192] {
            let pieces: Vec<_> = (0..size)
                .map(|index| lengths.validate_piece_index(index).unwrap())
                .collect();
            let streams = Arc::new(TorrentStreams::default());
            streams.set_piece_request_queue(Some(pieces.clone()));
            let iterations = (1_000_000 / (size * size)).clamp(8, 1000);
            let same = streams.clone();
            let original = pieces.clone();
            measure(
                "identical",
                size,
                iterations,
                Box::new(move || {
                    black_box(same.set_piece_request_queue(Some(original.clone())));
                }),
            );
            let mut reversed = pieces.clone();
            reversed.reverse();
            let reorder = streams.clone();
            let mut alternate = false;
            measure(
                "reorder",
                size,
                iterations,
                Box::new(move || {
                    alternate = !alternate;
                    let next = if alternate { &reversed } else { &pieces };
                    black_box(reorder.set_piece_request_queue(Some(next.clone())));
                }),
            );
            measure(
                "missing-membership",
                size,
                10_000,
                Box::new(move || {
                    black_box(streams.is_piece_requested(lengths.last_piece_id()));
                }),
            );
        }
    }

    #[tokio::test]
    async fn caller_queue_keeps_order_and_only_changed_requests_wake_peers() {
        let lengths = Lengths::new(100 * 1024 * 1024, 1024 * 1024).unwrap();
        let streams = TorrentStreams::default();
        let pieces = |ids: &[u32]| {
            ids.iter()
                .map(|&i| lengths.validate_piece_index(i).unwrap())
                .collect()
        };
        assert!(streams.set_piece_request_queue(Some(pieces(&[50, 2, 51]))));
        assert_eq!(
            streams
                .iter_next_pieces(&lengths)
                .map(|p| p.get())
                .collect::<Vec<_>>(),
            [50, 2, 51]
        );
        let wake = streams.new_pieces_notify.notified();
        tokio::pin!(wake);
        assert!(futures::poll!(wake.as_mut()).is_pending());
        assert!(!streams.set_piece_request_queue(Some(pieces(&[50, 2, 51]))));
        assert!(futures::poll!(wake.as_mut()).is_pending());
        assert!(!streams.set_piece_request_queue(Some(pieces(&[2, 50, 51]))));
        assert!(futures::poll!(wake.as_mut()).is_ready());
        assert!(!streams.set_piece_request_queue(Some(Vec::new())));
        assert!(streams.iter_next_pieces(&lengths).next().is_none());
        assert!(streams.set_piece_request_queue(None));
    }
}
