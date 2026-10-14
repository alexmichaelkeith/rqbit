use super::*;
use librqbit_core::dns::ResolveFuture;
use std::sync::Mutex;

struct Resolver {
    destination: Option<SocketAddr>,
    calls: Arc<Mutex<Vec<(String, u16)>>>,
}

impl HostResolver for Resolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        Box::pin(async move {
            self.calls.lock().unwrap().push((host.to_owned(), port));
            match self.destination {
                Some(addr) => Ok(vec![addr]),
                None => anyhow::bail!("injected bootstrap failure"),
            }
        })
    }
}

#[tokio::test]
async fn bootstrap_uses_injected_resolution_to_send_a_real_query() {
    let endpoint = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = endpoint.local_addr().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let cancel = CancellationToken::new();
    let dht = DhtState::with_config(DhtConfig {
        listen_addr: Some((std::net::Ipv4Addr::LOCALHOST, 0).into()),
        bootstrap_addrs: Some(vec![format!("bootstrap.invalid:{}", addr.port())]),
        resolver: Some(Arc::new(Resolver {
            destination: Some(addr),
            calls: calls.clone(),
        })),
        cancellation_token: Some(cancel.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut buffer = [0u8; 2048];
    let (n, from) = tokio::time::timeout(Duration::from_secs(5), endpoint.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(from, dht.listen_addr());
    let message = bprotocol::deserialize_message::<buffers::ByteBufOwned>(&buffer[..n]).unwrap();
    assert!(matches!(
        message.kind,
        bprotocol::MessageKind::FindNodeRequest(_)
    ));
    assert_eq!(
        calls.lock().unwrap()[0],
        ("bootstrap.invalid".to_owned(), addr.port())
    );
    cancel.cancel();
}

#[tokio::test]
async fn bootstrap_resolution_errors_do_not_fall_back_to_a_literal_address() {
    let endpoint = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let cancel = CancellationToken::new();
    let _dht = DhtState::with_config(DhtConfig {
        listen_addr: Some((std::net::Ipv4Addr::LOCALHOST, 0).into()),
        bootstrap_addrs: Some(vec![endpoint.local_addr().unwrap().to_string()]),
        resolver: Some(Arc::new(Resolver {
            destination: None,
            calls: calls.clone(),
        })),
        cancellation_token: Some(cancel.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while calls.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut buffer = [0u8; 2048];
    assert!(
        tokio::time::timeout(Duration::from_millis(150), endpoint.recv_from(&mut buffer))
            .await
            .is_err()
    );
    cancel.cancel();
}
