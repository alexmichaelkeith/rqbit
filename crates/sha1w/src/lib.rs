// Wrapper for sha1/sha256 libraries to be able to swap them easily,
// e.g. to measure performance, or change implementations depending on platform.
//
// Keep hashing independent of TLS so embedders can use a Rust implementation
// without selecting another TLS cryptography provider.

pub trait ISha1 {
    fn new() -> Self;
    fn update(&mut self, buf: &[u8]);
    fn finish(self) -> [u8; 20];
}

/// SHA-256 hash trait for BEP 52 (BitTorrent v2) support.
pub trait ISha256 {
    fn new() -> Self;
    fn update(&mut self, buf: &[u8]);
    fn finish(self) -> [u8; 32];

    fn finish_id32(self) -> [u8; 32]
    where
        Self: Sized,
    {
        self.finish()
    }
}

assert_cfg::exactly_one! {
    feature = "sha1-crypto-hash",
    feature = "sha1-ring",
    feature = "sha1-rustcrypto",
}

#[cfg(feature = "sha1-rustcrypto")]
mod rustcrypto_impl {
    use super::{ISha1, ISha256};
    use sha1::Digest;

    pub struct Sha1RustCrypto(sha1::Sha1);
    impl ISha1 for Sha1RustCrypto {
        fn new() -> Self {
            Self(sha1::Sha1::new())
        }
        fn update(&mut self, buf: &[u8]) {
            self.0.update(buf);
        }
        fn finish(self) -> [u8; 20] {
            self.0.finalize().into()
        }
    }

    pub struct Sha256RustCrypto(sha2::Sha256);
    impl ISha256 for Sha256RustCrypto {
        fn new() -> Self {
            Self(sha2::Sha256::new())
        }
        fn update(&mut self, buf: &[u8]) {
            self.0.update(buf);
        }
        fn finish(self) -> [u8; 32] {
            self.0.finalize().into()
        }
    }
}

#[cfg(feature = "sha1-rustcrypto")]
pub type Sha1 = rustcrypto_impl::Sha1RustCrypto;
#[cfg(feature = "sha1-rustcrypto")]
pub type Sha256 = rustcrypto_impl::Sha256RustCrypto;

#[cfg(feature = "sha1-crypto-hash")]
mod crypto_hash_impl {
    use super::{ISha1, ISha256};

    pub struct Sha1CryptoHash {
        inner: crypto_hash::Hasher,
    }

    impl ISha1 for Sha1CryptoHash {
        fn new() -> Self {
            Self {
                inner: crypto_hash::Hasher::new(crypto_hash::Algorithm::SHA1),
            }
        }

        fn update(&mut self, buf: &[u8]) {
            use std::io::Write;
            self.inner.write_all(buf).unwrap();
        }

        fn finish(mut self) -> [u8; 20] {
            let result = self.inner.finish();
            debug_assert_eq!(result.len(), 20);
            let mut result_arr = [0u8; 20];
            result_arr.copy_from_slice(&result);
            result_arr
        }
    }

    pub struct Sha256CryptoHash {
        inner: crypto_hash::Hasher,
    }

    impl ISha256 for Sha256CryptoHash {
        fn new() -> Self {
            Self {
                inner: crypto_hash::Hasher::new(crypto_hash::Algorithm::SHA256),
            }
        }

        fn update(&mut self, buf: &[u8]) {
            use std::io::Write;
            self.inner.write_all(buf).unwrap();
        }

        fn finish(mut self) -> [u8; 32] {
            let result = self.inner.finish();
            debug_assert_eq!(result.len(), 32);
            let mut result_arr = [0u8; 32];
            result_arr.copy_from_slice(&result);
            result_arr
        }
    }
}

#[cfg(feature = "sha1-ring")]
mod ring_impl {
    use super::{ISha1, ISha256};

    use aws_lc_rs::digest::{Context, SHA1_FOR_LEGACY_USE_ONLY as SHA1, SHA256};

    pub struct Sha1Ring {
        ctx: Context,
    }

    impl ISha1 for Sha1Ring {
        fn new() -> Self {
            Self {
                ctx: Context::new(&SHA1),
            }
        }

        fn update(&mut self, buf: &[u8]) {
            self.ctx.update(buf);
        }

        fn finish(self) -> [u8; 20] {
            let result = self.ctx.finish();
            debug_assert_eq!(result.as_ref().len(), 20);
            let mut result_arr = [0u8; 20];
            result_arr.copy_from_slice(result.as_ref());
            result_arr
        }
    }

    pub struct Sha256Ring {
        ctx: Context,
    }

    impl ISha256 for Sha256Ring {
        fn new() -> Self {
            Self {
                ctx: Context::new(&SHA256),
            }
        }

        fn update(&mut self, buf: &[u8]) {
            self.ctx.update(buf);
        }

        fn finish(self) -> [u8; 32] {
            let result = self.ctx.finish();
            debug_assert_eq!(result.as_ref().len(), 32);
            let mut result_arr = [0u8; 32];
            result_arr.copy_from_slice(result.as_ref());
            result_arr
        }
    }
}

#[cfg(feature = "sha1-crypto-hash")]
pub type Sha1 = crypto_hash_impl::Sha1CryptoHash;

#[cfg(feature = "sha1-ring")]
pub type Sha1 = ring_impl::Sha1Ring;

#[cfg(feature = "sha1-crypto-hash")]
pub type Sha256 = crypto_hash_impl::Sha256CryptoHash;

#[cfg(feature = "sha1-ring")]
pub type Sha256 = ring_impl::Sha256Ring;

#[cfg(test)]
mod tests {
    use super::{ISha1, ISha256, Sha1, Sha256};

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn standard_vectors_and_incremental_updates() {
        for (data, expected_sha1, expected_sha256) in [
            (
                Vec::new(),
                "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                b"abc".to_vec(),
                "a9993e364706816aba3e25717850c26c9cd0d89d",
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                vec![b'a'; 1_000_000],
                "34aa973cd4c4daa4f61eeb2bdbad27316534016f",
                "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
            ),
        ] {
            for chunk in [1, 63, 64, 65, 16 * 1024, 1024 * 1024] {
                let mut sha1 = Sha1::new();
                let mut sha256 = Sha256::new();
                for bytes in data.chunks(chunk) {
                    sha1.update(bytes);
                    sha256.update(bytes);
                }
                assert_eq!(hex(&sha1.finish()), expected_sha1);
                assert_eq!(hex(&sha256.finish()), expected_sha256);
            }
        }
    }

    fn assert_sha256_impl<T: ISha256>() {}

    #[test]
    fn test_sha256_known_vector_empty() {
        assert_sha256_impl::<Sha256>();
        let mut h = Sha256::new();
        h.update(b"");
        let got = h.finish();
        let expected: [u8; 32] = [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ];
        assert_eq!(got, expected);
    }
}
