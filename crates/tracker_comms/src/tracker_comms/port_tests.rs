use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    sync::mpsc,
    time::{advance, timeout},
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn changed_ports_obey_minimum_and_coalesce_to_latest_value() {
    let (tx, mut rx) = watch::channel(Some(41000));
    let last = Instant::now();
    tx.send_replace(Some(41001));
    {
        let wait = TrackerComms::sleep_until_interval_or_port_change(
            &mut rx,
            Duration::from_secs(3600),
            Some(last),
            Duration::from_secs(12),
        );
        tokio::pin!(wait);
        assert!(futures::poll!(wait.as_mut()).is_pending());
        advance(Duration::from_secs(5)).await;
        tx.send_replace(Some(41002));
        assert!(futures::poll!(wait.as_mut()).is_pending());
        advance(Duration::from_secs(7)).await;
        wait.await;
    }
    assert_eq!(TrackerComms::announce_port_from(&mut rx), 41002);
    assert!(
        !rx.has_changed().unwrap(),
        "the announced update must be consumed"
    );

    drop(tx);
    let wait = TrackerComms::sleep_until_interval_or_port_change(
        &mut rx,
        Duration::from_secs(60),
        Some(Instant::now()),
        Duration::ZERO,
    );
    tokio::pin!(wait);
    assert!(futures::poll!(wait.as_mut()).is_pending());
    advance(Duration::from_secs(59)).await;
    assert!(futures::poll!(wait.as_mut()).is_pending());
    advance(Duration::from_secs(1)).await;
    wait.await;
}

async fn drive_tracker(
    url: Url,
    port_rx: watch::Receiver<Option<u16>>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    // The test binary must also work when a parent selects provider-free TLS.
    // This does not choose a provider for users of tracker-comms.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let udp = UdpTrackerClient::new(cancel, None).await.unwrap();
    let mut peers = TrackerComms::start_with_options(
        "0000000000000000000000000000000000000000".parse().unwrap(),
        "1111111111111111111111111111111111111111".parse().unwrap(),
        [url].into_iter().collect(),
        Box::new(()),
        None,
        port_rx,
        reqwest::Client::builder().no_proxy().build().unwrap(),
        udp,
        None,
    )
    .unwrap();
    tokio::spawn(async move { while peers.next().await.is_some() {} })
}

#[tokio::test]
async fn http_monitor_reannounces_latest_port_and_honors_tracker_minimum() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = Url::parse(&format!(
        "http://{}/announce",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..n]);
            }
            let request = std::str::from_utf8(&request).unwrap();
            let target = request.split_whitespace().nth(1).unwrap();
            let parsed = Url::parse(&format!("http://localhost{target}")).unwrap();
            let port: u16 = parsed
                .query_pairs()
                .find(|(key, _)| key == "port")
                .unwrap()
                .1
                .parse()
                .unwrap();
            seen_tx.send((port, Instant::now())).unwrap();
            let body = b"d8:intervali3600e12:min intervali6e5:peers0:e";
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        }
    });
    let cancel = CancellationToken::new();
    let (tx, rx) = watch::channel(Some(41000));
    let monitor = drive_tracker(url, rx, cancel.clone()).await;
    let first = timeout(Duration::from_secs(5), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.0, 41000);
    tx.send_replace(Some(41001));
    tx.send_replace(Some(41002));
    assert!(
        timeout(Duration::from_secs(5), seen_rx.recv())
            .await
            .is_err()
    );
    let second = timeout(Duration::from_secs(4), seen_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.0, 41002);
    assert!(second.1.duration_since(first.1) >= Duration::from_millis(5800));
    assert!(
        timeout(Duration::from_millis(150), seen_rx.recv())
            .await
            .is_err()
    );
    cancel.cancel();
    monitor.abort();
    server.abort();
    let _ = monitor.await;
    let _ = server.await;
}

#[tokio::test]
async fn udp_monitor_reannounces_latest_port_on_the_wire() {
    let socket = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = Url::parse(&format!("udp://{}/announce", socket.local_addr().unwrap())).unwrap();
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let mut buffer = [0u8; 1024];
        loop {
            let (n, addr) = socket.recv_from(&mut buffer).await.unwrap();
            let action = u32::from_be_bytes(buffer[8..12].try_into().unwrap());
            let mut response = Vec::new();
            response.extend_from_slice(&buffer[8..16]); // action and transaction ID
            match action {
                0 => response.extend_from_slice(&42u64.to_be_bytes()),
                1 => {
                    assert_eq!(n, 98);
                    seen_tx
                        .send(u16::from_be_bytes(buffer[96..98].try_into().unwrap()))
                        .unwrap();
                    response.extend_from_slice(&3600u32.to_be_bytes());
                    response.extend_from_slice(&[0; 8]);
                }
                _ => panic!("unexpected tracker action"),
            }
            socket.send_to(&response, addr).await.unwrap();
        }
    });
    let cancel = CancellationToken::new();
    let (tx, rx) = watch::channel(Some(41000));
    let monitor = drive_tracker(url, rx, cancel.clone()).await;
    assert_eq!(
        timeout(Duration::from_secs(5), seen_rx.recv())
            .await
            .unwrap(),
        Some(41000)
    );
    tx.send_replace(Some(41001));
    tx.send_replace(Some(41002));
    assert_eq!(
        timeout(Duration::from_secs(8), seen_rx.recv())
            .await
            .unwrap(),
        Some(41002)
    );
    assert!(
        timeout(Duration::from_millis(150), seen_rx.recv())
            .await
            .is_err()
    );
    cancel.cancel();
    monitor.abort();
    server.abort();
    let _ = monitor.await;
    let _ = server.await;
}
