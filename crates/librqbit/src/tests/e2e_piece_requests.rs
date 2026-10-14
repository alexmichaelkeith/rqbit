use std::{net::Ipv4Addr, time::Duration};

use anyhow::{Context, ensure};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::time::{sleep, timeout};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerOptions, Session, SessionOptions,
    create_torrent, spawn_utils::BlockingSpawner,
};

use super::test_util::{create_default_random_dir_with_torrents, setup_test_logging, wait_until};

#[tokio::test(flavor = "multi_thread")]
async fn cross_file_requests_survive_pause_and_keep_selected_file_completion() -> anyhow::Result<()>
{
    timeout(Duration::from_secs(30), async {
        setup_test_logging();
        let piece_length = 32 * 1024;
        let original = TempDir::new()?;
        let first = vec![0x11; 2 * piece_length + piece_length / 2];
        let second = (0..2 * piece_length + piece_length / 2)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>();
        std::fs::write(original.path().join("0.data"), &first)?;
        std::fs::write(original.path().join("1.data"), &second)?;
        let torrent = create_torrent(
            original.path(),
            CreateTorrentOptions {
                piece_length: Some(u32::try_from(piece_length)?),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?;
        let options = |listen| SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: if listen {
                Some(ListenerOptions {
                    listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                })
            } else {
                None
            },
            ..Default::default()
        };
        let server = Session::new_with_opts(original.path().into(), options(true)).await?;
        let seed = server
            .add_torrent(
                AddTorrent::from_bytes(torrent.as_bytes()?),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(original.path().to_string_lossy().into_owned()),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("seed handle")?;
        seed.wait_until_initialized().await?;
        let metadata = seed.metadata.load_full().context("seed metadata")?;
        let selected_file = metadata
            .file_infos
            .iter()
            .position(|file| file.offset_in_torrent != 0)
            .context("second file")?;
        ensure!(metadata.file_infos[selected_file].offset_in_torrent == first.len() as u64);
        let selected_bytes = if metadata.file_infos[selected_file]
            .relative_filename
            .ends_with("1.data")
        {
            &second
        } else {
            &first
        };
        seed.wait_until_completed().await?;

        let destination = TempDir::new()?;
        let client = Session::new_with_opts(destination.path().into(), options(false)).await?;
        let handle = client
            .add_torrent(
                AddTorrent::from_bytes(torrent.as_bytes()?),
                Some(AddTorrentOptions {
                    paused: true,
                    only_files: Some(vec![selected_file]),
                    initial_peers: Some(vec![server.listen_addr().unwrap()]),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("client handle")?;
        handle.wait_until_initialized().await?;
        let metadata = handle.metadata.load_full().context("client metadata")?;
        let piece = |index| metadata.lengths().validate_piece_index(index).unwrap();
        let assert_have = |expected: &[u32]| -> anyhow::Result<()> {
            handle.with_chunk_tracker(|chunks| {
                for index in 0..5 {
                    ensure!(
                        chunks.is_piece_have(piece(index)) == expected.contains(&index),
                        "unexpected availability for piece {index}"
                    );
                }
                Ok(())
            })?
        };
        handle.set_background_completion(false)?;
        handle.set_piece_request_queue(Some(vec![piece(2)]))?;
        client.unpause(&handle).await?;
        wait_until(|| assert_have(&[2]), Duration::from_secs(5)).await?;
        let mut reader = handle.clone().stream(selected_file).await?;
        let mut prefix = vec![0; piece_length / 2];
        reader.read_exact(&mut prefix).await?;
        ensure!(
            prefix == selected_bytes[..prefix.len()],
            "incorrect bytes from shared piece"
        );

        client.pause(&handle).await?;
        handle.with_state(|state| {
            let streams = match state {
                crate::ManagedTorrentState::Paused(state) => &state.streams,
                crate::ManagedTorrentState::Live(state) => &state.streams,
                _ => anyhow::bail!("expected live or paused state"),
            };
            ensure!(
                streams
                    .iter_next_pieces(metadata.lengths())
                    .collect::<Vec<_>>()
                    == vec![piece(2)]
            );
            Ok::<_, anyhow::Error>(())
        })?;
        // Reader lookahead now points into piece 3. Replace demand while paused
        // and verify that the queue and disabled completion survive resumption.
        handle.set_piece_request_queue(Some(vec![piece(3)]))?;
        client.unpause(&handle).await?;
        handle.with_state(|state| {
            let streams = match state {
                crate::ManagedTorrentState::Paused(state) => &state.streams,
                crate::ManagedTorrentState::Live(state) => &state.streams,
                _ => anyhow::bail!("expected live or paused state"),
            };
            ensure!(
                streams
                    .iter_next_pieces(metadata.lengths())
                    .collect::<Vec<_>>()
                    == vec![piece(3)]
            );
            Ok::<_, anyhow::Error>(())
        })?;
        wait_until(|| assert_have(&[2, 3]), Duration::from_secs(5)).await?;
        handle.set_piece_request_queue(Some(Vec::new()))?;
        handle.set_background_completion(true)?;
        handle.wait_until_completed().await?;
        assert_have(&[2, 3, 4])?;
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).await?;
        ensure!(
            remaining == selected_bytes[prefix.len()..],
            "incorrect bytes after pause/resume"
        );
        drop(reader);
        client.stop().await;
        server.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread")]
async fn caller_piece_queue_restricts_downloads_and_restores_native_scheduling()
-> anyhow::Result<()> {
    timeout(Duration::from_secs(30), exercise_piece_requests()).await?
}

async fn exercise_piece_requests() -> anyhow::Result<()> {
    setup_test_logging();
    let piece_length = 32 * 1024;
    let files = create_default_random_dir_with_torrents(1, 16 * piece_length, None);
    let torrent = create_torrent(
        files.path(),
        CreateTorrentOptions {
            piece_length: Some(u32::try_from(piece_length)?),
            ..Default::default()
        },
        &BlockingSpawner::new(1),
    )
    .await?;
    let torrent_bytes = torrent.as_bytes()?;
    let server = Session::new_with_opts(
        files.path().into(),
        SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await?;
    let seed = server
        .add_torrent(
            AddTorrent::from_bytes(torrent_bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(files.path().to_string_lossy().into_owned()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected seed handle")?;
    timeout(Duration::from_secs(5), seed.wait_until_completed()).await??;

    for restore_reader_scheduling in [false, true] {
        let destination = TempDir::new()?;
        let client = Session::new_with_opts(
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
        let handle = client
            .add_torrent(
                AddTorrent::from_bytes(torrent_bytes.clone()),
                Some(AddTorrentOptions {
                    paused: true,
                    initial_peers: Some(vec![server.listen_addr().context("seed listener")?]),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected client handle")?;
        handle.wait_until_initialized().await?;
        let metadata = handle.metadata.load_full().context("torrent metadata")?;
        let piece = |index| metadata.lengths().validate_piece_index(index).unwrap();
        let assert_verified = |expected: &[u32]| -> anyhow::Result<()> {
            handle.with_chunk_tracker(|chunks| {
                for index in 0..16 {
                    ensure!(
                        chunks.is_piece_have(piece(index)) == expected.contains(&index),
                        "unexpected availability for piece {index}"
                    );
                }
                Ok(())
            })?
        };
        handle.set_background_completion(false)?;
        handle.set_piece_request_queue(Some(Vec::new()))?;

        // An open native reader would normally request the entire small file.
        // The explicit empty queue must suppress that lookahead.
        let reader = handle.clone().stream(0).await?;
        client.unpause(&handle).await?;
        sleep(Duration::from_millis(150)).await;
        ensure!(
            handle.stats().progress_bytes == 0,
            "empty queue fetched data"
        );
        assert_verified(&[])?;

        handle.set_piece_request_queue(Some(vec![piece(15), piece(2)]))?;
        // An index validated against a larger torrent must not corrupt this queue.
        let foreign = crate::Lengths::new(17 * piece_length as u64, u32::try_from(piece_length)?)?
            .validate_piece_index(16)
            .unwrap();
        let error = handle
            .set_piece_request_queue(Some(vec![piece(7), foreign]))
            .unwrap_err();
        ensure!(error.to_string().contains("outside this torrent"));
        wait_until(
            || {
                ensure!(handle.stats().progress_bytes == 2 * piece_length as u64);
                Ok(())
            },
            Duration::from_secs(5),
        )
        .await?;
        sleep(Duration::from_millis(150)).await;
        ensure!(handle.stats().progress_bytes == 2 * piece_length as u64);
        assert_verified(&[15, 2])?;

        // New demand must wake/reconnect peers that exhausted the old queue.
        handle.set_piece_request_queue(Some(vec![piece(7)]))?;
        wait_until(
            || {
                ensure!(handle.stats().progress_bytes == 3 * piece_length as u64);
                Ok(())
            },
            Duration::from_secs(5),
        )
        .await?;
        handle.set_piece_request_queue(Some(Vec::new()))?;
        sleep(Duration::from_millis(150)).await;
        ensure!(handle.stats().progress_bytes == 3 * piece_length as u64);
        assert_verified(&[15, 2, 7])?;

        if restore_reader_scheduling {
            handle.set_piece_request_queue(None)?;
        } else {
            handle.set_background_completion(true)?;
        }
        timeout(Duration::from_secs(5), handle.wait_until_completed()).await??;
        drop(reader);
        client.stop().await;
    }
    server.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_reader_advances_beyond_lookahead_without_background_completion()
-> anyhow::Result<()> {
    timeout(Duration::from_secs(30), async {
        setup_test_logging();
        let files = create_default_random_dir_with_torrents(1, 96 * 1024 * 1024, None);
        let torrent = create_torrent(
            files.path(),
            CreateTorrentOptions {
                piece_length: Some(256 * 1024),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?;
        let server = Session::new_with_opts(
            files.path().into(),
            SessionOptions {
                dht: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: Some(ListenerOptions {
                    listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?;
        let seed = server
            .add_torrent(
                AddTorrent::from_bytes(torrent.as_bytes()?),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(files.path().to_string_lossy().into_owned()),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .unwrap();
        seed.wait_until_completed().await?;
        let destination = TempDir::new()?;
        let client = Session::new_with_opts(
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
        let handle = client
            .add_torrent(
                AddTorrent::from_bytes(torrent.as_bytes()?),
                Some(AddTorrentOptions {
                    paused: true,
                    initial_peers: Some(vec![server.listen_addr().unwrap()]),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await?;
        handle.set_background_completion(false)?;
        let mut reader = handle.clone().stream(0).await?;
        client.unpause(&handle).await?;
        let mut actual = vec![0; 40 * 1024 * 1024];
        reader.read_exact(&mut actual).await?;
        let expected = std::fs::read(files.path().join("0.data"))?;
        ensure!(
            actual == expected[..actual.len()],
            "native reader returned incorrect bytes"
        );
        ensure!(
            handle.stats().progress_bytes < handle.stats().total_bytes,
            "background completion unexpectedly fetched the tail"
        );
        drop(reader);
        client.stop().await;
        server.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread")]
async fn replacing_demand_preserves_inflight_work_from_multiple_peers() -> anyhow::Result<()> {
    timeout(Duration::from_secs(30), async {
        setup_test_logging();
        let piece_length = 256 * 1024;
        let original = TempDir::new()?;
        let mut contents = vec![0u8; 8 * piece_length];
        for (index, piece) in contents.chunks_mut(piece_length).enumerate() {
            piece.fill(u8::try_from(index + 1)?);
        }
        let original_file = original.path().join("payload.bin");
        std::fs::write(&original_file, &contents)?;
        let torrent = create_torrent(
            &original_file,
            CreateTorrentOptions {
                piece_length: Some(u32::try_from(piece_length)?),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?;
        let mut seeds = Vec::new();
        let mut seed_dirs = Vec::new();
        for index in [1, 5] {
            let directory = TempDir::new()?;
            let mut sparse = vec![0; contents.len()];
            let range = index * piece_length..(index + 1) * piece_length;
            sparse[range.clone()].copy_from_slice(&contents[range]);
            std::fs::write(directory.path().join("payload.bin"), sparse)?;
            let session = Session::new_with_opts(
                directory.path().into(),
                SessionOptions {
                    dht: None,
                    disable_trackers: true,
                    disable_local_service_discovery: true,
                    listen: Some(ListenerOptions {
                        listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                        ..Default::default()
                    }),
                    ratelimits: crate::limits::LimitsConfig {
                        upload_bps: std::num::NonZeroU32::new(64 * 1024),
                        download_bps: None,
                    },
                    ..Default::default()
                },
            )
            .await?;
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(torrent.as_bytes()?),
                    Some(AddTorrentOptions {
                        overwrite: true,
                        only_files: Some(Vec::new()),
                        output_folder: Some(directory.path().to_string_lossy().into_owned()),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .unwrap();
            handle.wait_until_initialized().await?;
            seeds.push(session);
            seed_dirs.push(directory);
        }
        let destination = TempDir::new()?;
        let client = Session::new_with_opts(
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
        let handle = client
            .add_torrent(
                AddTorrent::from_bytes(torrent.as_bytes()?),
                Some(AddTorrentOptions {
                    paused: true,
                    initial_peers: Some(
                        seeds
                            .iter()
                            .map(|session| session.listen_addr().unwrap())
                            .collect(),
                    ),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await?;
        let metadata = handle.metadata.load_full().unwrap();
        let piece = |index| metadata.lengths().validate_piece_index(index).unwrap();
        handle.set_background_completion(false)?;
        handle.set_piece_request_queue(Some(vec![piece(1), piece(5)]))?;
        client.unpause(&handle).await?;
        wait_until(
            || {
                let live = handle.live().unwrap();
                let state = live.lock_read("test_multiple_assignments");
                let pieces = state.get_pieces()?;
                let first = pieces
                    .get_inflight(piece(1))
                    .context("first piece not assigned")?;
                let second = pieces
                    .get_inflight(piece(5))
                    .context("second piece not assigned")?;
                ensure!(first.peer != second.peer, "expected different peers");
                Ok(())
            },
            Duration::from_secs(8),
        )
        .await?;
        // Neither seed has piece 3. Already-sent work must still finish after this update.
        handle.set_piece_request_queue(Some(vec![piece(3)]))?;
        wait_until(
            || {
                ensure!(handle.stats().progress_bytes == 2 * piece_length as u64);
                Ok(())
            },
            Duration::from_secs(12),
        )
        .await?;
        handle.with_chunk_tracker(|chunks| {
            for index in 0..8 {
                ensure!(chunks.is_piece_have(piece(index)) == [1, 5].contains(&index));
            }
            Ok::<_, anyhow::Error>(())
        })??;
        client.stop().await;
        for seed in seeds {
            seed.stop().await;
        }
        drop(seed_dirs);
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
