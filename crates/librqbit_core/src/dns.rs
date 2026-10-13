use std::{future::Future, net::SocketAddr, pin::Pin};

pub type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<Vec<SocketAddr>>> + Send + 'a>>;

/// DNS resolution used by torrent networking.
///
/// Implementations are responsible for selecting and binding their own
/// transport. Callers must not fall back to the system resolver after an
/// implementation returns an error.
pub trait HostResolver: Send + Sync {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a>;
}
