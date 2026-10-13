//! Mac/loopback playback fixture: generated media, a seeder and a tiny tracker.
//! No DHT, LAN discovery, external tracker or external peer is used.
use librqbit::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions,
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let source = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or_else(|| anyhow::anyhow!("media file required"))?,
    );
    let root = source.parent().unwrap();
    let seed_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 18757));
    let http_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 18756));
    let metadata = librqbit::create_torrent(
        &source,
        CreateTorrentOptions {
            trackers: vec![format!("http://{http_addr}/announce")],
            piece_length: Some(256 * 1024),
            ..Default::default()
        },
        &librqbit::spawn_utils::BlockingSpawner::new(1),
    )
    .await?;
    let bytes = metadata.as_bytes()?;
    tokio::fs::write(root.join("fixture.torrent"), &bytes).await?;
    let session = Session::new_with_opts(
        root.to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                listen_addr: seed_addr,
                mode: ListenerMode::TcpOnly,
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ratelimits: librqbit::limits::LimitsConfig {
                upload_bps: NonZeroU32::new(1024 * 1024),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                disable_trackers: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .ok_or_else(|| anyhow::anyhow!("no seed handle"))?;
    handle.wait_until_initialized().await?;
    let manifest = serde_json::json!({"info_hash": metadata.info_hash().as_string(), "torrent_url":format!("http://{http_addr}/fixture.torrent"), "seed_address":seed_addr.to_string(), "size":std::fs::metadata(&source)?.len(), "duration_seconds":600});
    tokio::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .await?;
    println!("Loopback seed ready: {manifest}");
    let listener = TcpListener::bind(http_addr).await?;
    let bytes = Arc::new(bytes);
    let mut tracker = b"d8:completei1e10:incompletei1e8:intervali2e5:peers6:".to_vec();
    tracker.extend([127, 0, 0, 1]);
    tracker.extend(seed_addr.port().to_be_bytes());
    tracker.push(b'e');
    let tracker = Arc::new(tracker);
    let mut tasks = JoinSet::new();
    let expires = tokio::time::Instant::now() + Duration::from_secs(30 * 60);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(expires) => break,
            result = listener.accept() => {
                let (mut socket, _) = result?; let bytes = bytes.clone(); let tracker = tracker.clone();
                tasks.spawn(async move {
                    let request = async {
                        let mut request = [0; 8192];
                        let len = socket.read(&mut request).await?;
                        let is_torrent = String::from_utf8_lossy(&request[..len]).starts_with("GET /fixture.torrent ");
                        let body: &[u8] = if is_torrent { &bytes } else { &tracker };
                        let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
                        socket.write_all(headers.as_bytes()).await?;
                        socket.write_all(body).await?;
                        Ok::<_,std::io::Error>(())
                    };
                    let _ = tokio::time::timeout(Duration::from_secs(5), request).await;
                });
            }
            _ = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
    tasks.abort_all();
    session.stop().await;
    Ok(())
}
