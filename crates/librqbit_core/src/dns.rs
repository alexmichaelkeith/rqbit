use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    time::Duration,
};

use anyhow::{Context, bail};
use librqbit_dualstack_sockets::{BindDevice, BindOpts, UdpSocket};

const DNS_PORT: u16 = 53;
const DNS_TIMEOUT: Duration = Duration::from_secs(5);
const DNS_HEADER_LEN: usize = 12;
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_AAAA: u16 = 28;
const DNS_CLASS_IN: u16 = 1;

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

/// A minimal DNS resolver whose UDP sockets are bound to a specific network
/// interface. This keeps tracker and DHT hostname lookups on the same
/// fail-closed path as the rest of the torrent session.
#[derive(Debug, Clone)]
pub struct BoundDnsResolver {
    server: SocketAddr,
    bind_device: BindDevice,
    ipv4_only: bool,
}

impl BoundDnsResolver {
    pub fn new(server: IpAddr, bind_device: BindDevice, ipv4_only: bool) -> Self {
        Self {
            server: SocketAddr::new(server, DNS_PORT),
            bind_device,
            ipv4_only,
        }
    }

    async fn query(&self, host: &str, record_type: u16) -> anyhow::Result<Vec<IpAddr>> {
        let transaction_id = rand::random();
        let query = encode_query(transaction_id, host, record_type)?;
        let bind_addr = match self.server {
            SocketAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
            SocketAddr::V6(_) => SocketAddr::from(([0u16; 8], 0)),
        };
        let socket = UdpSocket::bind_udp(
            bind_addr,
            BindOpts {
                request_dualstack: false,
                reuseport: false,
                device: Some(&self.bind_device),
            },
        )
        .context("error creating VPN-bound DNS socket")?;

        tokio::time::timeout(DNS_TIMEOUT, socket.send_to(&query, self.server))
            .await
            .context("timed out sending VPN-bound DNS query")?
            .context("error sending VPN-bound DNS query")?;

        let mut response = [0u8; 4096];
        let (size, source) = tokio::time::timeout(DNS_TIMEOUT, socket.recv_from(&mut response))
            .await
            .context("timed out waiting for VPN-bound DNS response")?
            .context("error receiving VPN-bound DNS response")?;
        if source != self.server {
            bail!(
                "VPN-bound DNS response came from unexpected server {source}, expected {}",
                self.server
            );
        }

        parse_response(&response[..size], transaction_id, record_type)
    }
}

impl HostResolver for BoundDnsResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        Box::pin(async move {
            if let Ok(ip) = host.parse::<IpAddr>() {
                return Ok(vec![SocketAddr::new(ip, port)]);
            }

            let mut addrs = self
                .query(host, DNS_TYPE_A)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect::<Vec<_>>();

            if !self.ipv4_only {
                addrs.extend(
                    self.query(host, DNS_TYPE_AAAA)
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, port)),
                );
            }

            if addrs.is_empty() {
                bail!(
                    "VPN-bound DNS server {} returned no usable addresses for {host}",
                    self.server
                );
            }
            Ok(addrs)
        })
    }
}

fn encode_query(transaction_id: u16, host: &str, record_type: u16) -> anyhow::Result<Vec<u8>> {
    let host = host.trim_end_matches('.');
    if host.is_empty() || host.len() > 253 {
        bail!("invalid DNS hostname");
    }

    let mut query = Vec::with_capacity(DNS_HEADER_LEN + host.len() + 6);
    query.extend_from_slice(&transaction_id.to_be_bytes());
    query.extend_from_slice(&0x0100u16.to_be_bytes()); // recursion desired
    query.extend_from_slice(&1u16.to_be_bytes()); // one question
    query.extend_from_slice(&0u16.to_be_bytes()); // answers
    query.extend_from_slice(&0u16.to_be_bytes()); // authority
    query.extend_from_slice(&0u16.to_be_bytes()); // additional
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            bail!("invalid DNS hostname label");
        }
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&record_type.to_be_bytes());
    query.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    Ok(query)
}

fn read_u16(packet: &[u8], offset: usize) -> anyhow::Result<u16> {
    let bytes = packet
        .get(offset..offset + 2)
        .context("truncated DNS response")?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn skip_name(packet: &[u8], mut offset: usize) -> anyhow::Result<usize> {
    for _ in 0..128 {
        let length = *packet.get(offset).context("truncated DNS name")?;
        if length & 0xc0 == 0xc0 {
            packet
                .get(offset..offset + 2)
                .context("truncated DNS compression pointer")?;
            return Ok(offset + 2);
        }
        if length & 0xc0 != 0 {
            bail!("invalid DNS name label");
        }
        offset += 1;
        if length == 0 {
            return Ok(offset);
        }
        offset = offset
            .checked_add(length as usize)
            .context("DNS name offset overflow")?;
        if offset > packet.len() {
            bail!("truncated DNS name");
        }
    }
    bail!("DNS name exceeded label limit")
}

fn parse_response(
    packet: &[u8],
    transaction_id: u16,
    record_type: u16,
) -> anyhow::Result<Vec<IpAddr>> {
    if packet.len() < DNS_HEADER_LEN {
        bail!("truncated DNS response header");
    }
    if read_u16(packet, 0)? != transaction_id {
        bail!("DNS transaction ID mismatch");
    }
    let flags = read_u16(packet, 2)?;
    if flags & 0x8000 == 0 {
        bail!("DNS packet is not a response");
    }
    if flags & 0x0200 != 0 {
        bail!("truncated DNS response is unsupported");
    }
    let response_code = flags & 0x000f;
    if response_code != 0 {
        bail!("DNS server returned response code {response_code}");
    }

    let question_count = read_u16(packet, 4)? as usize;
    let answer_count = read_u16(packet, 6)? as usize;
    let mut offset = DNS_HEADER_LEN;
    for _ in 0..question_count {
        offset = skip_name(packet, offset)?;
        packet
            .get(offset..offset + 4)
            .context("truncated DNS question")?;
        offset += 4;
    }

    let mut addresses = Vec::new();
    for _ in 0..answer_count {
        offset = skip_name(packet, offset)?;
        let answer_type = read_u16(packet, offset)?;
        let class = read_u16(packet, offset + 2)?;
        let data_len = read_u16(packet, offset + 8)? as usize;
        offset += 10;
        let data = packet
            .get(offset..offset + data_len)
            .context("truncated DNS answer")?;
        if class == DNS_CLASS_IN && answer_type == record_type {
            match (answer_type, data) {
                (DNS_TYPE_A, [a, b, c, d]) => {
                    addresses.push(IpAddr::V4(Ipv4Addr::new(*a, *b, *c, *d)));
                }
                (DNS_TYPE_AAAA, data) if data.len() == 16 => {
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(data);
                    addresses.push(IpAddr::V6(Ipv6Addr::from(octets)));
                }
                _ => {}
            }
        }
        offset += data_len;
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_a_query() {
        let query = encode_query(0x1234, "tracker.example", DNS_TYPE_A).unwrap();
        assert_eq!(&query[..2], &[0x12, 0x34]);
        assert_eq!(
            &query[DNS_HEADER_LEN..],
            &[
                7, b't', b'r', b'a', b'c', b'k', b'e', b'r', 7, b'e', b'x', b'a', b'm', b'p', b'l',
                b'e', 0, 0, 1, 0, 1
            ]
        );
    }

    #[test]
    fn parses_compressed_a_response() {
        let mut packet = encode_query(0x1234, "tracker.example", DNS_TYPE_A).unwrap();
        packet[2..4].copy_from_slice(&0x8180u16.to_be_bytes());
        packet[6..8].copy_from_slice(&1u16.to_be_bytes());
        packet.extend_from_slice(&[
            0xc0, 0x0c, // compressed name
            0x00, 0x01, // A
            0x00, 0x01, // IN
            0x00, 0x00, 0x00, 0x3c, // TTL
            0x00, 0x04, // data length
            192, 0, 2, 1,
        ]);
        assert_eq!(
            parse_response(&packet, 0x1234, DNS_TYPE_A).unwrap(),
            vec![IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))]
        );
    }

    #[test]
    fn rejects_wrong_transaction() {
        let packet = encode_query(0x1234, "tracker.example", DNS_TYPE_A).unwrap();
        assert!(parse_response(&packet, 0x9999, DNS_TYPE_A).is_err());
    }
}
