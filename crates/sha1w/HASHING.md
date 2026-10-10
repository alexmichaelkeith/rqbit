# Hashing backends

Select exactly one hashing feature with default features disabled:
`sha1-crypto-hash` (system crypto), `sha1-ring` (AWS-LC, historical name), or
`sha1-rustcrypto` (RustCrypto SHA-1 and SHA-256, without native crypto).

Embedders of `librqbit` can enable `rust-tls-no-provider` and
`sha1-rustcrypto` together, then install their own Rustls crypto provider.
The existing `rust-tls` feature retains its original provider/hash selection.

Compare throughput with
`cargo run -p librqbit-sha1-wrapper --release --example throughput --no-default-features --features sha1-rustcrypto`
and repeat with `sha1-ring` on the same machine.
