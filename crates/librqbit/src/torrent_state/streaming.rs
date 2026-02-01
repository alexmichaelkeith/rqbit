use std::{
    collections::VecDeque,
    io::SeekFrom,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Instant,
};

use anyhow::Context;
use dashmap::DashMap;

use librqbit_core::lengths::{CurrentPiece, Lengths, ValidPieceIndex};
use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::OwnedSemaphorePermit,
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
    /// Optional priority pieces for this stream. When set, these pieces are
    /// yielded first in the stream's queue, before the normal lookahead pieces.
    /// This is used for seek prioritization without affecting other streams.
    priority_pieces: Option<Vec<ValidPieceIndex>>,
}

impl StreamState {
    fn current_piece(&self, lengths: &Lengths) -> Option<CurrentPiece> {
        lengths.compute_current_piece(self.position, self.file_abs_offset)
    }

    /// Returns only the normal lookahead pieces (without priority pieces).
    /// Priority pieces are handled separately by iter_next_pieces to ensure they 
    /// take absolute precedence over ALL streams' non-priority pieces.
    fn queue_without_priority(&self, lengths: &Lengths) -> std::vec::IntoIter<ValidPieceIndex> {
        let start = self.file_abs_offset + self.position;
        let end = (start + PER_STREAM_BUF_DEFAULT).min(self.file_abs_offset + self.file_len);
        let dpl = lengths.default_piece_length();
        let start_id = (start / dpl as u64).try_into().unwrap();
        let end_id = end.div_ceil(dpl as u64).try_into().unwrap();
        let normal_pieces: Vec<_> = (start_id..end_id)
            .filter_map(|i| lengths.validate_piece_index(i))
            .collect();
        normal_pieces.into_iter()
    }
}

#[derive(Default)]
pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    streams: DashMap<StreamId, StreamState>,
}

impl TorrentStreams {
    fn next_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the number of active streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    fn register_waker(&self, stream_id: StreamId, waker: Waker) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let vm = s.value_mut();
            let position = vm.position;
            vm.waker = Some(waker);
            debug!(
                stream_id,
                position,
                "registered waker for stream at position {}",
                position
            );
        }
    }

    // Interleave 1st, 2nd etc pieces from each active stream in turn until they get 1/10th of the file.
    // 
    // IMPORTANT: Priority pieces from ANY stream are yielded FIRST before any non-priority pieces.
    // This ensures that seek operations (which set priority pieces) get immediate attention
    // even when other streams are active at different positions.
    pub(crate) fn iter_next_pieces<'a>(
        &'a self,
        lengths: &'a Lengths,
    ) -> impl Iterator<Item = ValidPieceIndex> + 'a {
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

        // Collect ALL priority pieces from ALL streams first - these take absolute precedence
        // Clone in the same step to avoid lifetime issues with DashMap iteration
        // IMPORTANT: 
        // 1. Deduplicate across streams! Multiple streams may share the same file/position
        //    and thus have identical priority pieces.
        // 2. INTERLEAVE priority pieces across streams! If we just flatten [header: 0,1,2,3] + [cues: 2112,2113],
        //    all header pieces get requested first and Cues only starts when slots free up.
        //    By interleaving (0, 2112, 1, 2113, 2, 3), header AND Cues get requested in parallel.
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        
        // Collect each stream's priority pieces separately for interleaving
        let priority_queues: Vec<Vec<ValidPieceIndex>> = self.streams.iter()
            .filter_map(|s| s.priority_pieces.clone())
            .collect();
        
        // Interleave priority pieces across streams (round-robin), then deduplicate
        let mut all_priority_pieces: Vec<ValidPieceIndex> = Vec::new();
        let max_len = priority_queues.iter().map(|q| q.len()).max().unwrap_or(0);
        for i in 0..max_len {
            for queue in &priority_queues {
                if let Some(&piece) = queue.get(i) {
                    if seen.insert(piece.get()) {  // Only yield each piece once
                        all_priority_pieces.push(piece);
                    }
                }
            }
        }
        
        // Collect normal queues (without priority pieces) for interleaving
        let mut normal_queues: Vec<_> = self.streams.iter()
            .map(|s| s.queue_without_priority(lengths))
            .collect();
        
        // Log what streams we have and their priority pieces (for seek/cold start debugging)
        if !all_priority_pieces.is_empty() {
            let stream_ids: Vec<_> = self.streams.iter().map(|s| *s.key()).collect();
            let has_priority: Vec<_> = self.streams.iter()
                .filter_map(|s| s.priority_pieces.as_ref().map(|p| (*s.key(), p.iter().map(|x| x.get()).collect::<Vec<_>>())))
                .collect();
            debug!(
                stream_count = normal_queues.len(),
                priority_piece_count = all_priority_pieces.len(),
                ?stream_ids,
                ?has_priority,
                "iter_next_pieces: yielding priority pieces first"
            );
        }

        // Shuffle normal queues to decrease determinism and make queueing fairer.
        use rand::seq::SliceRandom;
        normal_queues.shuffle(&mut rand::rng());

        // Yield priority pieces first, then interleave normal pieces
        all_priority_pieces.into_iter().chain(Interleave { all: normal_queues.into() })
    }

    pub(crate) fn wake_streams_on_piece_completed(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) {
        let stream_count = self.streams.len();
        if piece_id.get() < 5 {
            // Log for first few pieces to debug cold start
            debug!(
                piece_id = piece_id.get(),
                stream_count,
                "wake_streams_on_piece_completed: checking {} streams",
                stream_count
            );
        }
        
        for mut w in self.streams.iter_mut() {
            let stream_id = *w.key();
            let current = w.value().current_piece(lengths);
            let current_piece_id = current.as_ref().map(|p| p.id);
            let has_waker = w.value().waker.is_some();
            
            if piece_id.get() < 5 {
                // Detailed debug for first few pieces
                debug!(
                    stream_id,
                    piece_id = piece_id.get(),
                    current_piece = ?current_piece_id.map(|p| p.get()),
                    has_waker,
                    position = w.value().position,
                    file_offset = w.value().file_abs_offset,
                    "stream state check"
                );
            }
            
            if current_piece_id == Some(piece_id)
                && let Some(waker) = w.value_mut().waker.take()
            {
                debug!(
                    stream_id,
                    piece_id = piece_id.get(),
                    "waking stream"
                );
                waker.wake();
            }
        }
    }

    fn drop_stream(&self, stream_id: StreamId) -> Option<StreamState> {
        debug!(stream_id, "dropping stream");
        self.streams.remove(&stream_id).map(|s| s.1)
    }

    pub(crate) fn streamed_file_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.streams.iter().map(|s| s.value().file_id)
    }

    /// Set priority pieces for a specific stream. These pieces will be requested
    /// before the normal lookahead pieces for this stream only.
    /// 
    /// This is opt-in: if never called, the stream behaves normally.
    /// Call with `None` to clear priority and return to normal behavior.
    /// 
    /// Use case: when a user seeks, set priority to the seek target piece(s)
    /// so they are downloaded first, without affecting other streams.
    pub fn set_stream_priority(&self, stream_id: StreamId, pieces: Option<Vec<ValidPieceIndex>>) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let piece_ids: Vec<u32> = pieces.as_ref()
                .map(|p| p.iter().map(|x| x.get()).collect())
                .unwrap_or_default();
            debug!(
                stream_id,
                ?piece_ids,
                "set_stream_priority: setting priority pieces"
            );
            s.value_mut().priority_pieces = pieces;
        } else {
            debug!(
                stream_id,
                "set_stream_priority: stream not found in DashMap!"
            );
        }
    }

    /// Get all priority pieces from all streams. This returns the actual
    /// priority_pieces that were set via set_stream_priority, not filtered
    /// by have/inflight status.
    pub fn get_all_priority_pieces(&self) -> Vec<ValidPieceIndex> {
        let mut all_priority: Vec<ValidPieceIndex> = Vec::new();
        for entry in self.streams.iter() {
            if let Some(ref pieces) = entry.value().priority_pieces {
                all_priority.extend(pieces.iter().cloned());
            }
        }
        all_priority
    }

    /// Get the stream ID for a given stream. Useful for callers who need to
    /// set priority on a stream they've opened.
    #[allow(dead_code)]
    pub fn get_stream_id_by_file(&self, file_id: usize) -> Option<StreamId> {
        self.streams.iter()
            .find(|s| s.value().file_id == file_id)
            .map(|s| *s.key())
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
                priority_pieces: None,
            },
        );

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
        self.streams
            .streams
            .get_mut(&self.stream_id)
            .unwrap()
            .value_mut()
            .position = new_pos;
    }

    pub fn len(&self) -> u64 {
        self.file_len
    }

    /// Get the stream ID for this stream.
    pub fn stream_id(&self) -> usize {
        self.stream_id
    }

    /// Set priority pieces for this stream. These pieces will be requested
    /// before the normal lookahead pieces, allowing faster seeking.
    /// 
    /// This is opt-in: if never called, the stream behaves normally.
    /// Call with `None` to clear priority and return to normal behavior.
    /// 
    /// Example: when seeking to a position, calculate the target piece(s)
    /// and call `set_priority(Some(vec![target_piece, target_piece + 1]))`.
    pub fn set_priority(&self, pieces: Option<Vec<ValidPieceIndex>>) {
        self.streams.set_stream_priority(self.stream_id, pieces);
    }
}
