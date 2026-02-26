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
    sync::{OwnedSemaphorePermit, broadcast},
};
use tracing::{debug, trace};

use crate::{ManagedTorrent, file_info::FileInfo, storage::TorrentStorage};

use super::{ManagedTorrentHandle, TorrentMetadata};

type StreamId = usize;

// 512 MB lookahead by default. With 16MB pieces, this covers ~32 pieces ahead
// of the current read position as "normal" (non-priority) lookahead. This prevents
// the download loop from falling through to natural_order_pieces (sequential from 
// position 0) when the priority window is exhausted.
const PER_STREAM_BUF_DEFAULT: u64 = 512 * 1024 * 1024;

// Rolling priority window size (number of pieces ahead to prioritize)
const COLD_START_PRIORITY_PIECES: u32 = 6;  // Smaller window for fast cold start
const STEADY_STATE_PRIORITY_PIECES: u32 = 15; // Larger window once playing to avoid stalls

// How many pieces ahead of current before we update the rolling window
// (avoids updating priority on every single read)
const PRIORITY_UPDATE_THRESHOLD_PIECES: u32 = 3;

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
    /// The starting piece of the current priority window (for rolling updates)
    priority_window_start: Option<u32>,
    /// Whether this stream has started playing (past cold start)
    is_playing: bool,
    /// The last time this stream was read from or waited on
    last_activity: Instant,
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

/// A persistent streaming anchor that outlives individual HTTP connections.
/// Registered by the application layer (e.g., OpenPVR) to tell the engine
/// "a user is watching at this position" — even when no FileStream is open.
///
/// This replaces the old "ghost position" hack. Instead of guessing from
/// dropped streams, the application explicitly manages anchors via
/// register/update/unregister calls tied to its own session lifecycle.
#[derive(Clone, Debug)]
pub struct StreamingAnchor {
    pub file_id: usize,
    pub file_abs_offset: u64,
    pub position: u64,
    pub file_len: u64,
    pub last_updated: Instant,
}

/// The capacity of the piece completion broadcast channel.
/// Subscribers that fall behind will skip missed pieces (lossy).
const PIECE_NOTIFY_CAPACITY: usize = 256;

pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    streams: DashMap<StreamId, StreamState>,
    /// Application-managed streaming anchors keyed by session ID.
    /// These persist independently of FileStream lifecycle and prevent
    /// fallback to natural_order_pieces (sequential from piece 0).
    anchors: DashMap<String, StreamingAnchor>,
    /// Broadcast channel for piece completion events.
    /// WebSocket handlers subscribe to this to push real-time piece progress
    /// to connected clients without polling.
    piece_completed_tx: broadcast::Sender<u32>,
}

impl Default for TorrentStreams {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(PIECE_NOTIFY_CAPACITY);
        Self {
            next_stream_id: AtomicUsize::new(0),
            streams: DashMap::new(),
            anchors: DashMap::new(),
            piece_completed_tx: tx,
        }
    }
}

impl TorrentStreams {
    fn next_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the number of active streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Returns true if there are live streams OR registered streaming anchors.
    /// This prevents the engine from falling back to natural_order_pieces
    /// (sequential from piece 0) when all browser connections are briefly closed.
    pub fn has_streaming_context(&self) -> bool {
        if self.streams.len() > 0 {
            return true;
        }
        // Check if we have any active anchors (registered by the application layer)
        !self.anchors.is_empty()
    }

    /// Register or update a streaming anchor for a session.
    /// Call this when a user starts streaming, and update it on heartbeats.
    /// The anchor ensures piece selection stays focused on the user's playback
    /// position even when all HTTP connections (FileStreams) are temporarily closed.
    pub fn register_streaming_anchor(
        &self,
        session_id: &str,
        file_id: usize,
        position: u64,
        file_abs_offset: u64,
        file_len: u64,
    ) {
        let is_new = !self.anchors.contains_key(session_id);
        self.anchors.insert(
            session_id.to_string(),
            StreamingAnchor {
                file_id,
                file_abs_offset,
                position,
                file_len,
                last_updated: Instant::now(),
            },
        );
        if is_new {
            tracing::info!(
                session_id,
                file_id,
                position,
                anchor_count = self.anchors.len(),
                "registered new streaming anchor"
            );
        } else {
            tracing::debug!(
                session_id,
                file_id,
                position,
                "updated streaming anchor position"
            );
        }
    }

    /// Remove a streaming anchor when a session ends.
    pub fn unregister_streaming_anchor(&self, session_id: &str) {
        if self.anchors.remove(session_id).is_some() {
            tracing::info!(
                session_id,
                anchor_count = self.anchors.len(),
                "unregistered streaming anchor"
            );
        }
    }

    /// Get the number of active streaming anchors.
    pub fn anchor_count(&self) -> usize {
        self.anchors.len()
    }

    /// Subscribe to piece completion events.
    /// Returns a broadcast receiver that yields the piece index (u32) of each
    /// completed piece. WebSocket handlers use this to push real-time progress.
    ///
    /// If the receiver falls behind, it will get a `Lagged` error and skip
    /// missed pieces — this is fine since the WS handler can query the full
    /// bitmap to catch up.
    pub fn subscribe_piece_completed(&self) -> broadcast::Receiver<u32> {
        self.piece_completed_tx.subscribe()
    }

    fn register_waker(&self, stream_id: StreamId, waker: Waker) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let vm = s.value_mut();
            let position = vm.position;
            vm.waker = Some(waker);
            vm.last_activity = Instant::now();
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
        // Group streams into "active" (read from in the last 2 seconds) and "inactive"
        let mut active_queues = Vec::new();
        let mut inactive_queues = Vec::new();
        
        let mut active_stream_debug = Vec::new();
        
        for s in self.streams.iter() {
            let queue = s.value().queue_without_priority(lengths);
            let has_waker = s.value().waker.is_some();
            let elapsed = s.value().last_activity.elapsed().as_secs();
            // A stream is only active if it was read from recently.
            // Even if it has a waker (is blocked), if the player hasn't polled it
            // in 2 seconds, we consider it inactive so it doesn't steal priority.
            let is_active = elapsed < 2;
            
            active_stream_debug.push(format!(
                "stream_id={} pos={} waker={} elapsed={}s -> active={}",
                s.key(), s.value().position, has_waker, elapsed, is_active
            ));
            
            if is_active {
                active_queues.push(queue);
            } else {
                // Only keep inactive streams that were active recently (e.g., within the last 30 seconds)
                // This prevents old, abandoned streams from piece 0 from hijacking the download queue
                // when the player's buffer fills up and the current stream becomes "inactive".
                if elapsed < 30 {
                    inactive_queues.push((elapsed, queue));
                }
            }
        }
        
        // If we have active streams, only interleave those.
        // Otherwise, sort inactive streams by how recently they were active, and only use the most recent one.
        // If there are NO streams at all, use the ghost position (last known playback position)
        // to generate a fallback queue so we don't revert to downloading from piece 0.
        let mut normal_queues = if !active_queues.is_empty() {
            active_queues
        } else if !inactive_queues.is_empty() {
            inactive_queues.sort_by_key(|(elapsed, _)| *elapsed);
            if let Some((_, most_recent_queue)) = inactive_queues.into_iter().next() {
                vec![most_recent_queue]
            } else {
                Vec::new()
            }
        } else {
            // No live streams at all — use streaming anchors if any exist.
            // Each anchor generates a lookahead queue from its position, just like a live stream would.
            // Multiple anchors = multiple users = interleaved queues.
            let anchor_queues: Vec<_> = self.anchors.iter()
                .map(|entry| {
                    let anchor = entry.value();
                    let start = anchor.file_abs_offset + anchor.position;
                    let end = (start + PER_STREAM_BUF_DEFAULT).min(anchor.file_abs_offset + anchor.file_len);
                    let dpl = lengths.default_piece_length();
                    let start_id = (start / dpl as u64).try_into().unwrap();
                    let end_id = end.div_ceil(dpl as u64).try_into().unwrap();
                    let pieces: Vec<_> = (start_id..end_id)
                        .filter_map(|i| lengths.validate_piece_index(i))
                        .collect();
                    pieces.into_iter()
                })
                .collect();
            if !anchor_queues.is_empty() {
                anchor_queues
            } else {
                Vec::new()
            }
        };
        
        // Log what streams we have and their priority pieces (for seek/cold start debugging)
        static LAST_LOG: AtomicUsize = AtomicUsize::new(0);
        let now_secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as usize;
        let last_log = LAST_LOG.swap(now_secs, Ordering::Relaxed);
        
        if now_secs - last_log >= 2 {
            let stream_ids: Vec<_> = self.streams.iter().map(|s| *s.key()).collect();
            let has_priority: Vec<_> = self.streams.iter()
                .filter_map(|s| s.priority_pieces.as_ref().map(|p| (*s.key(), p.iter().map(|x| x.get()).collect::<Vec<_>>())))
                .collect();
            
            let anchor_info: Vec<_> = self.anchors.iter()
                .map(|entry| {
                    let a = entry.value();
                    let piece = (a.file_abs_offset + a.position) / lengths.default_piece_length() as u64;
                    format!("{}:piece={}", entry.key(), piece)
                })
                .collect();
                
            tracing::info!(
                stream_count = self.streams.len(),
                active_queues = normal_queues.len(),
                priority_piece_count = all_priority_pieces.len(),
                anchor_count = self.anchors.len(),
                ?anchor_info,
                ?stream_ids,
                ?has_priority,
                streams_status = ?active_stream_debug,
                "iter_next_pieces: stream status check"
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
        // Broadcast piece completion to any WebSocket subscribers.
        // Ignore send errors — means no subscribers are listening.
        let _ = self.piece_completed_tx.send(piece_id.get());

        let stream_count = self.streams.len();
        let anchor_count = self.anchors.len();
        
        // Always log at debug, but also at info for seek debugging when streams are waiting
        debug!(
            piece_id = piece_id.get(),
            stream_count,
            anchor_count,
            "piece completed, checking {} streams",
            stream_count
        );
        
        let mut woke_count = 0usize;
        
        for mut w in self.streams.iter_mut() {
            let stream_id = *w.key();
            let current = w.value().current_piece(lengths);
            let current_piece_id = current.as_ref().map(|p| p.id);
            let has_waker = w.value().waker.is_some();
            let position = w.value().position;
            let file_offset = w.value().file_abs_offset;
            
            if current_piece_id == Some(piece_id)
                && let Some(waker) = w.value_mut().waker.take()
            {
                tracing::debug!(
                    stream_id,
                    piece_id = piece_id.get(),
                    position,
                    position_mb = position / 1024 / 1024,
                    "✅ WAKING STREAM: piece matches current read position"
                );
                waker.wake();
                woke_count += 1;
            } else if has_waker {
                // Stream is waiting but not for this piece — log for diagnostics
                debug!(
                    stream_id,
                    completed_piece = piece_id.get(),
                    waiting_for_piece = ?current_piece_id.map(|p| p.get()),
                    position,
                    position_mb = position / 1024 / 1024,
                    file_offset,
                    "stream waiting for different piece"
                );
            }
        }
        
        if stream_count > 0 && woke_count == 0 {
            debug!(
                piece_id = piece_id.get(),
                stream_count,
                "piece completed but no streams were waiting for it"
            );
        }
    }

    fn drop_stream(&self, stream_id: StreamId) -> Option<StreamState> {
        debug!(stream_id, "dropping stream");
        let removed = self.streams.remove(&stream_id).map(|s| s.1);
        
        if let Some(ref state) = removed {
            tracing::info!(
                stream_id,
                position = state.position,
                file_abs_offset = state.file_abs_offset,
                remaining_streams = self.streams.len(),
                anchor_count = self.anchors.len(),
                "stream dropped (anchors maintain piece selection context)"
            );
        }
        
        removed
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
            tracing::info!(
                stream_id,
                ?piece_ids,
                "set_stream_priority: setting priority pieces"
            );
            
            let state = s.value_mut();
            state.priority_pieces = pieces;
            
            // If we're setting new priority pieces, anchor the rolling window to the first piece
            // so that subsequent seek() calls don't immediately overwrite these carefully chosen pieces.
            if let Some(ref p) = state.priority_pieces {
                if let Some(first_piece) = p.first() {
                    state.priority_window_start = Some(first_piece.get());
                }
            }
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
    
    /// Enable rolling priority for a stream. This sets up the initial priority window
    /// and enables automatic window updates as the read position advances.
    /// 
    /// Call this after cold start priority pieces are downloaded to switch from
    /// static priority to rolling priority mode.
    #[allow(dead_code)]
    pub fn enable_rolling_priority(&self, stream_id: StreamId, current_piece: u32, lengths: &Lengths) {
        if let Some(mut s) = self.streams.get_mut(&stream_id) {
            let state = s.value_mut();
            state.priority_window_start = Some(current_piece);
            state.is_playing = false; // Will become true after COLD_START_PRIORITY_PIECES
            
            // Set initial priority window
            let window_size = COLD_START_PRIORITY_PIECES;
            let mut new_priority: Vec<ValidPieceIndex> = Vec::with_capacity(window_size as usize);
            
            for i in 0..window_size {
                let piece_id = current_piece + i;
                if let Some(valid_piece) = lengths.validate_piece_index(piece_id) {
                    new_priority.push(valid_piece);
                }
            }
            
            if !new_priority.is_empty() {
                debug!(
                    stream_id,
                    current_piece,
                    window_size = new_priority.len(),
                    "enabled rolling priority with initial window"
                );
                state.priority_pieces = Some(new_priority);
            }
        }
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

        // Log every poll_read call at debug level so we can see if it's being called at all.
        // For the first call after a seek or creation, log at info level.
        tracing::debug!(
            stream_id = self.stream_id,
            file_id = self.file_id,
            piece_id = current.id.get(),
            position = self.position,
            position_mb = self.position / 1024 / 1024,
            piece_remaining = current.piece_remaining,
            "poll_read called"
        );

        // if the piece is not there, register to wake when it is
        // check if we have the piece for real
        let have = poll_try_io!(self.torrent.with_chunk_tracker(|ct| {
            let have = ct.get_have_pieces().as_slice()[current.id.get() as usize];
            if !have {
                self.streams
                    .register_waker(self.stream_id, cx.waker().clone());
                // Log at info level for seek debugging — we need visibility into why
                // a stream is blocked when the user can see pieces in the bitmap.
                tracing::debug!(
                    stream_id = self.stream_id,
                    file_id = self.file_id,
                    piece_id = current.id.get(),
                    position = self.position,
                    position_mb = self.position / 1024 / 1024,
                    file_offset = self.file_torrent_abs_offset,
                    "⏳ STREAM BLOCKED: waiting for piece (not in chunk_tracker)"
                );
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
        tracing::debug!(
            stream_id = self.stream_id,
            position = self.position,
            position_mb = self.position / 1024 / 1024,
            file_id = self.file_id,
            "🎯 FileStream SEEK to position"
        );
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
        tracing::info!(
            stream_id = self.stream_id,
            file_id = self.file_id,
            position = self.position,
            position_mb = self.position / 1024 / 1024,
            file_len_mb = self.file_len / 1024 / 1024,
            "🗑️ FileStream DROPPED (position={} MB / {} MB)",
            self.position / 1024 / 1024,
            self.file_len / 1024 / 1024
        );
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

    /// Register or update a streaming anchor for a session.
    ///
    /// Call this from the application layer (e.g., on session registration or heartbeat)
    /// to tell the engine that a user is streaming `file_id` at byte `position`.
    /// The engine will keep downloading pieces around this position even when
    /// all HTTP connections (FileStreams) are temporarily closed.
    ///
    /// For multi-user: each session_id gets its own anchor, so multiple users
    /// streaming the same torrent at different positions are all served.
    pub fn register_streaming_anchor(
        &self,
        session_id: &str,
        file_id: usize,
        position: u64,
    ) -> anyhow::Result<()> {
        let metadata = self
            .metadata
            .load_full()
            .context("torrent metadata is not resolved")?;
        let fi = metadata
            .file_infos
            .get(file_id)
            .context("invalid file_id for streaming anchor")?;
        let streams = self.streams()?;
        streams.register_streaming_anchor(
            session_id,
            file_id,
            position,
            fi.offset_in_torrent,
            fi.len,
        );
        Ok(())
    }

    /// Remove a streaming anchor when a session ends or times out.
    pub fn unregister_streaming_anchor(&self, session_id: &str) -> anyhow::Result<()> {
        let streams = self.streams()?;
        streams.unregister_streaming_anchor(session_id);
        Ok(())
    }

    /// Subscribe to piece completion events for this torrent.
    /// Returns a broadcast receiver that yields piece indices as they complete.
    /// Used by WebSocket handlers to push real-time download progress.
    pub fn subscribe_piece_completed(&self) -> anyhow::Result<broadcast::Receiver<u32>> {
        let streams = self.streams()?;
        Ok(streams.subscribe_piece_completed())
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
                priority_window_start: None,
                is_playing: false,
                last_activity: Instant::now(),
            },
        );

        debug!(stream_id = s.stream_id, file_id, "started stream");
        tracing::debug!(
            stream_id = s.stream_id,
            file_id,
            file_len_mb = fd_len / 1024 / 1024,
            file_offset = fd_offset,
            total_streams = streams.streams.len(),
            "📡 NEW FileStream created (total active: {})",
            streams.streams.len()
        );

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
        
        let lengths = self.metadata.lengths();
        let piece_len = lengths.default_piece_length() as u64;
        
        // Calculate current piece based on absolute file offset
        let abs_pos = self.file_torrent_abs_offset + new_pos;
        let current_piece = (abs_pos / piece_len) as u32;
        
        // Update stream state and check if we need to roll the priority window
        if let Some(mut state) = self.streams.streams.get_mut(&self.stream_id) {
            let state = state.value_mut();
            state.position = new_pos;
            state.last_activity = Instant::now();
            
            // Check if we need to update the rolling priority window
            let should_update = match state.priority_window_start {
                Some(window_start) => {
                    if current_piece < window_start {
                        true
                    } else {
                        // Find the end of the current priority window
                        let window_end = state.priority_pieces.as_ref()
                            .and_then(|p| p.last())
                            .map(|p| p.get())
                            .unwrap_or(window_start);
                        
                        // If we are getting close to the end of the priority window, roll it forward.
                        // "Close" means less than PRIORITY_UPDATE_THRESHOLD_PIECES left.
                        if current_piece > window_end {
                            true
                        } else {
                            let pieces_left = window_end.saturating_sub(current_piece);
                            pieces_left < PRIORITY_UPDATE_THRESHOLD_PIECES
                        }
                    }
                }
                None => {
                    // No priority window set yet - this stream isn't using rolling priority
                    // (priority was set externally, e.g., by OpenPVR's cold start logic)
                    false
                }
            };
            
            if should_update {
                // Mark as playing once we've advanced past the first few pieces
                // We need to check relative to the window start, not absolute piece 0
                if let Some(window_start) = state.priority_window_start {
                    if current_piece >= window_start + COLD_START_PRIORITY_PIECES {
                        state.is_playing = true;
                    }
                }
                
                // Calculate new priority window
                let window_size = if state.is_playing {
                    STEADY_STATE_PRIORITY_PIECES
                } else {
                    COLD_START_PRIORITY_PIECES
                };
                
                let mut new_priority: Vec<ValidPieceIndex> = Vec::with_capacity(window_size as usize);
                
                for i in 0..window_size {
                    let piece_id = current_piece + i;
                    if let Some(valid_piece) = lengths.validate_piece_index(piece_id) {
                        new_priority.push(valid_piece);
                    }
                }
                
                if !new_priority.is_empty() {
                    let first = new_priority.first().map(|p| p.get()).unwrap_or(0);
                    let last = new_priority.last().map(|p| p.get()).unwrap_or(0);
                    
                    // Only update & log if the window actually changed
                    let window_changed = state.priority_window_start != Some(current_piece)
                        || state.priority_pieces.as_ref().map(|p| p.len()) != Some(new_priority.len());
                    
                    if window_changed {
                        tracing::info!(
                            stream_id = self.stream_id,
                            current_piece,
                            window_start = first,
                            window_end = last,
                            window_size = new_priority.len(),
                            is_playing = state.is_playing,
                            "rolling priority window forward"
                        );
                        state.priority_window_start = Some(current_piece);
                        state.priority_pieces = Some(new_priority);
                    }
                }
            }
        }
    }
    
    /// Enable rolling priority for this stream starting at the current position.
    /// This should be called after cold start is complete to enable automatic
    /// priority window updates as playback progresses.
    pub fn enable_rolling_priority(&self) {
        let lengths = self.metadata.lengths();
        let piece_len = lengths.default_piece_length() as u64;
        let abs_pos = self.file_torrent_abs_offset + self.position;
        let current_piece = (abs_pos / piece_len) as u32;
        
        if let Some(mut state) = self.streams.streams.get_mut(&self.stream_id) {
            let state = state.value_mut();
            
            // If priority pieces were already set (e.g. by OpenPVR for a seek),
            // don't overwrite them. Just anchor the rolling window to the first piece.
            if let Some(ref p) = state.priority_pieces {
                if let Some(first_piece) = p.first() {
                    state.priority_window_start = Some(first_piece.get());
                    state.is_playing = false;
                    tracing::info!(
                        stream_id = self.stream_id,
                        current_piece,
                        window_start = first_piece.get(),
                        "enabled rolling priority (anchored to existing priority pieces)"
                    );
                    return;
                }
            }
            
            state.priority_window_start = Some(current_piece);
            state.is_playing = false; // Will become true after advancing past cold start
            
            // Calculate initial priority window
            let window_size = COLD_START_PRIORITY_PIECES;
            let mut new_priority: Vec<ValidPieceIndex> = Vec::with_capacity(window_size as usize);
            
            for i in 0..window_size {
                let piece_id = current_piece + i;
                if let Some(valid_piece) = lengths.validate_piece_index(piece_id) {
                    new_priority.push(valid_piece);
                }
            }
            
            if !new_priority.is_empty() {
                state.priority_pieces = Some(new_priority);
            }
            
            tracing::info!(
                stream_id = self.stream_id,
                current_piece,
                "enabled rolling priority (created new window)"
            );
        }
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
