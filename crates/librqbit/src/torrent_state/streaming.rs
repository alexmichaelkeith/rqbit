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

use librqbit_core::lengths::{CurrentPiece, Lengths, ValidPieceIndex};
use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::{Notify, OwnedSemaphorePermit, broadcast},
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
    priority_pieces: Option<Vec<ValidPieceIndex>>,
    read_ahead_limit: Option<usize>,
}

impl StreamState {
    fn current_piece(&self, lengths: &Lengths) -> Option<CurrentPiece> {
        lengths.compute_current_piece(self.position, self.file_abs_offset)
    }

    fn queue(&self, lengths: &Lengths) -> std::vec::IntoIter<ValidPieceIndex> {
        // Explicit demand is authoritative. Reading or seeking never replaces it.
        if let Some(pieces) = &self.priority_pieces {
            return pieces
                .iter()
                .copied()
                .take(self.read_ahead_limit.unwrap_or(usize::MAX))
                .collect::<Vec<_>>()
                .into_iter();
        }
        // Retain upstream's ordinary reader behavior for callers that do not
        // supply explicit demand. A caller may cap this mechanical lookahead.
        let start = self.file_abs_offset + self.position;
        let end = start
            .saturating_add(PER_STREAM_BUF_DEFAULT)
            .min(self.file_abs_offset + self.file_len);
        let dpl = lengths.default_piece_length();
        let start_id = (start / dpl as u64) as u32;
        let end_id = end.div_ceil(dpl as u64) as u32;
        (start_id..end_id)
            .filter_map(|i| lengths.validate_piece_index(i))
            .take(self.read_ahead_limit.unwrap_or(usize::MAX))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    streams: DashMap<StreamId, StreamState>,
    piece_demands: DashMap<String, Vec<ValidPieceIndex>>,
    piece_completed_tx: broadcast::Sender<u32>,
    pub(crate) new_pieces_notify: Notify,
    background_download: AtomicBool,
}

impl Default for TorrentStreams {
    fn default() -> Self {
        let (piece_completed_tx, _) = broadcast::channel(256);
        Self {
            next_stream_id: AtomicUsize::new(0),
            streams: DashMap::new(),
            piece_demands: DashMap::new(),
            piece_completed_tx,
            new_pieces_notify: Notify::new(),
            // Ordinary rqbit downloads retain upstream's completion behavior.
            background_download: AtomicBool::new(true),
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

    /// Interleave caller-owned demands and deduplicate shared pieces. There is
    /// no playback inference, activity timeout, or engine-owned window policy.
    pub(crate) fn iter_next_pieces(
        &self,
        lengths: &Lengths,
    ) -> impl Iterator<Item = ValidPieceIndex> + '_ {
        let mut queues: VecDeque<_> = self
            .streams
            .iter()
            .map(|s| s.queue(lengths))
            .chain(
                self.piece_demands
                    .iter()
                    .map(|d| d.value().clone().into_iter()),
            )
            .collect();
        let mut seen = HashSet::new();
        std::iter::from_fn(move || {
            while let Some(mut queue) = queues.pop_front() {
                if let Some(piece) = queue.next() {
                    queues.push_back(queue);
                    if seen.insert(piece) {
                        return Some(piece);
                    }
                }
            }
            None
        })
    }

    fn set_stream_priority(&self, stream_id: StreamId, pieces: Option<Vec<ValidPieceIndex>>) {
        if let Some(mut stream) = self.streams.get_mut(&stream_id) {
            stream.priority_pieces = pieces;
        }
        // Wake after releasing the map guard; peers read the same demand map.
        self.new_pieces_notify.notify_waiters();
    }

    fn set_read_ahead_limit(&self, stream_id: StreamId, pieces: Option<usize>) {
        if let Some(mut stream) = self.streams.get_mut(&stream_id) {
            stream.read_ahead_limit = pieces.map(|n| n.max(1));
        }
        self.new_pieces_notify.notify_waiters();
    }

    fn set_piece_demand(&self, owner: String, pieces: Option<Vec<ValidPieceIndex>>) {
        match pieces {
            Some(pieces) => {
                self.piece_demands.insert(owner, pieces);
            }
            None => {
                self.piece_demands.remove(&owner);
            }
        }
        self.new_pieces_notify.notify_waiters();
    }

    fn clear_piece_demands(&self) {
        self.piece_demands.clear();
        self.new_pieces_notify.notify_waiters();
    }

    pub(crate) fn get_all_priority_pieces(&self) -> Vec<ValidPieceIndex> {
        let mut seen = HashSet::new();
        self.streams
            .iter()
            .flat_map(|s| {
                s.priority_pieces
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .take(s.read_ahead_limit.unwrap_or(usize::MAX))
            })
            .chain(self.piece_demands.iter().flat_map(|d| d.value().clone()))
            .filter(|piece| seen.insert(*piece))
            .collect()
    }

    pub(crate) fn background_download_enabled(&self) -> bool {
        self.background_download.load(Ordering::Relaxed)
    }

    pub(crate) fn wake_streams_on_piece_completed(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) {
        let _ = self.piece_completed_tx.send(piece_id.get());
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
        self.new_pieces_notify.notify_waiters();
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

    /// Replace the exact pieces requested by this live reader.
    pub fn set_priority_for_stream(
        &self,
        stream_id: usize,
        pieces: Option<Vec<ValidPieceIndex>>,
    ) -> anyhow::Result<()> {
        self.streams()?.set_stream_priority(stream_id, pieces);
        Ok(())
    }

    /// Replace demand owned by the embedding application. Owners must release
    /// their demand explicitly; the engine does not guess playback lifetimes.
    pub fn set_piece_demand(
        &self,
        owner: String,
        pieces: Option<Vec<ValidPieceIndex>>,
    ) -> anyhow::Result<()> {
        self.streams()?.set_piece_demand(owner, pieces);
        self.with_state(|state| {
            if let crate::ManagedTorrentState::Live(l) = state {
                l.reconnect_all_not_needed_peers();
            }
        });
        Ok(())
    }

    pub fn clear_piece_demands(&self) -> anyhow::Result<()> {
        self.streams()?.clear_piece_demands();
        Ok(())
    }

    /// When disabled, fetch only pieces requested by readers or named demands.
    /// When enabled, remaining selected-file pieces use upstream scheduling.
    pub fn set_background_download(&self, enabled: bool) -> anyhow::Result<()> {
        let streams = self.streams()?;
        if streams.background_download.swap(enabled, Ordering::Relaxed) != enabled {
            streams.new_pieces_notify.notify_waiters();
        }
        Ok(())
    }

    pub fn subscribe_piece_completed(&self) -> anyhow::Result<broadcast::Receiver<u32>> {
        Ok(self.streams()?.piece_completed_tx.subscribe())
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
                priority_pieces: None,
                read_ahead_limit: None,
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
        self.streams
            .streams
            .get_mut(&self.stream_id)
            .unwrap()
            .value_mut()
            .position = new_pos;
        self.streams.new_pieces_notify.notify_waiters();
    }

    pub fn stream_id(&self) -> usize {
        self.stream_id
    }

    pub fn set_priority(&self, pieces: Option<Vec<ValidPieceIndex>>) {
        self.streams.set_stream_priority(self.stream_id, pieces);
    }

    pub fn set_read_ahead_limit(&self, pieces: Option<usize>) {
        self.streams.set_read_ahead_limit(self.stream_id, pieces);
    }

    pub fn len(&self) -> u64 {
        self.file_len
    }
}

#[cfg(test)]
mod demand_tests {
    use super::*;

    fn reader() -> StreamState {
        StreamState {
            file_id: 0,
            file_len: 100 * 1024 * 1024,
            file_abs_offset: 0,
            position: 0,
            waker: None,
            priority_pieces: None,
            read_ahead_limit: None,
        }
    }

    fn ids(streams: &TorrentStreams, lengths: &Lengths) -> Vec<u32> {
        streams.iter_next_pieces(lengths).map(|p| p.get()).collect()
    }

    #[test]
    fn explicit_demand_is_not_replaced_by_read_position() {
        let lengths = Lengths::new(100 * 1024 * 1024, 1024 * 1024).unwrap();
        let streams = TorrentStreams::default();
        streams.streams.insert(1, reader());
        let pieces = [50, 2, 51]
            .map(|p| lengths.validate_piece_index(p).unwrap())
            .to_vec();
        streams.set_stream_priority(1, Some(pieces));
        for position in [0, 12, 90] {
            streams.streams.get_mut(&1).unwrap().position = position * 1024 * 1024;
            assert_eq!(ids(&streams, &lengths), [50, 2, 51]);
        }
        streams.set_stream_priority(1, Some(Vec::new()));
        assert!(ids(&streams, &lengths).is_empty());
    }

    #[test]
    fn owners_share_pieces_and_release_independently() {
        let lengths = Lengths::new(100 * 1024 * 1024, 1024 * 1024).unwrap();
        let streams = TorrentStreams::default();
        let pieces = |indices: &[u32]| {
            indices
                .iter()
                .map(|&i| lengths.validate_piece_index(i).unwrap())
                .collect()
        };
        streams.streams.insert(1, reader());
        streams.set_stream_priority(1, Some(pieces(&[2, 3])));
        streams.set_piece_demand("viewer:a".into(), Some(pieces(&[50, 3])));
        streams.set_piece_demand("viewer:b".into(), Some(pieces(&[80])));
        let selected = ids(&streams, &lengths);
        assert_eq!(selected.len(), 4);
        assert_eq!(selected.iter().filter(|&&i| i == 3).count(), 1);
        assert!(selected.iter().position(|&i| i == 80).unwrap() < 3);
        streams.drop_stream(1);
        streams.set_piece_demand("viewer:a".into(), None);
        assert_eq!(ids(&streams, &lengths), [80]);
        streams.clear_piece_demands();
        assert!(ids(&streams, &lengths).is_empty());
    }

    #[test]
    fn reader_budget_survives_updates_without_limiting_siblings() {
        let lengths = Lengths::new(100 * 1024 * 1024, 1024 * 1024).unwrap();
        let streams = TorrentStreams::default();
        streams.streams.insert(1, reader());
        streams.streams.insert(2, reader());
        streams.set_read_ahead_limit(1, Some(2));
        for start in [0, 12, 50, 8] {
            let pieces = (start..start + 5)
                .map(|i| lengths.validate_piece_index(i).unwrap())
                .collect::<Vec<_>>();
            streams.set_stream_priority(1, Some(pieces.clone()));
            streams.set_stream_priority(2, Some(pieces));
            assert_eq!(streams.streams.get(&1).unwrap().queue(&lengths).count(), 2);
            assert_eq!(streams.streams.get(&2).unwrap().queue(&lengths).count(), 5);
        }
        streams.set_read_ahead_limit(1, None);
        assert_eq!(streams.streams.get(&1).unwrap().queue(&lengths).count(), 5);
    }
}
