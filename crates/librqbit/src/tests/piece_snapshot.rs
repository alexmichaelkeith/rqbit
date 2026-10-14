use std::{net::Ipv4Addr, time::Duration};

use super::test_util::{create_default_random_dir_with_torrents, setup_test_logging};
use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, PieceState, Session, SessionOptions,
    create_torrent,
    piece_tracker::{AcquireRequest, AcquireResult},
    spawn_utils::BlockingSpawner,
};

#[tokio::test]
async fn snapshots_distinguish_queued_assigned_verified_and_paused_pieces() -> anyhow::Result<()> {
    setup_test_logging();
    let files = create_default_random_dir_with_torrents(1, 4 * 32768, None);
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            piece_length: Some(32768),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let destination = tempfile::tempdir()?;
    let session = Session::new_with_opts(
        destination.path().into(),
        SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: None,
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.as_bytes()?),
            Some(AddTorrentOptions {
                paused: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.wait_until_initialized()).await??;
    assert_eq!(
        handle.get_piece_snapshot().unwrap().states,
        [PieceState::Missing; 4]
    );
    session.unpause(&handle).await?;
    let metadata = handle.metadata.load_full().unwrap();
    let piece = metadata.lengths().validate_piece_index(1).unwrap();
    let live = handle.live().unwrap();
    assert_eq!(
        handle.get_piece_snapshot().unwrap().states,
        [PieceState::Queued; 4]
    );
    {
        // Exercise the real assignment tracker without a network peer or timing race.
        let mut state = live.lock_write("snapshot_test_assign");
        let assigned = state.get_pieces_mut()?.acquire_piece(AcquireRequest {
            peer: (Ipv4Addr::LOCALHOST, 1).into(),
            peer_avg_time: None,
            priority_pieces: [piece].into_iter(),
            file_priorities: &Vec::new(),
            file_infos: &metadata.file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| true,
        });
        assert!(matches!(assigned, AcquireResult::Reserved(p) if p == piece));
    }
    let snapshot = handle.get_piece_snapshot().unwrap();
    assert_eq!(snapshot.piece_length, 32768);
    assert_eq!(
        snapshot.states,
        [
            PieceState::Queued,
            PieceState::InFlight,
            PieceState::Queued,
            PieceState::Queued
        ]
    );
    {
        let mut state = live.lock_write("snapshot_test_verify");
        let pieces = state.get_pieces_mut()?;
        pieces.take_inflight(piece);
        pieces.mark_piece_hash_ok(piece);
    }
    assert_eq!(
        handle.get_piece_snapshot().unwrap().states[1],
        PieceState::Verified
    );
    session.pause(&handle).await?;
    assert_eq!(
        handle.get_piece_snapshot().unwrap().states,
        [
            PieceState::Missing,
            PieceState::Verified,
            PieceState::Missing,
            PieceState::Missing
        ]
    );
    session.unpause(&handle).await?;
    assert_eq!(
        handle.get_piece_snapshot().unwrap().states,
        [
            PieceState::Queued,
            PieceState::Verified,
            PieceState::Queued,
            PieceState::Queued
        ]
    );
    session.stop().await;
    Ok(())
}
