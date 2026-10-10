//! Run with each backend's feature in release mode; no disk/network I/O.
use librqbit_sha1_wrapper::{ISha1, ISha256, Sha1, Sha256};
use std::{hint::black_box, time::Instant};

fn main() {
    let data = vec![0x5a; 1024 * 1024];
    for size in [16 * 1024, 1024 * 1024] {
        let loops = 256 * 1024 * 1024 / size;
        let start = Instant::now();
        for _ in 0..loops {
            let mut h = Sha1::new();
            h.update(black_box(&data[..size]));
            black_box(h.finish());
        }
        println!(
            "SHA1 {size} bytes: {:.0} MiB/s",
            256.0 / start.elapsed().as_secs_f64()
        );
        let start = Instant::now();
        for _ in 0..loops {
            let mut h = Sha256::new();
            h.update(black_box(&data[..size]));
            black_box(h.finish());
        }
        println!(
            "SHA256 {size} bytes: {:.0} MiB/s",
            256.0 / start.elapsed().as_secs_f64()
        );
    }
}
