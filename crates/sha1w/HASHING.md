# Hashing backends

Select exactly one hashing feature with default features disabled:
`sha1-crypto-hash` (system crypto), `sha1-ring` (AWS-LC, historical name), or
`sha1-rustcrypto` (RustCrypto SHA-1 and SHA-256, without native crypto).

Embedders of `librqbit` can enable `rust-tls-no-provider` and
`sha1-rustcrypto` together, then install their own Rustls crypto provider.
The existing `rust-tls` feature retains its original provider/hash selection.

The Rust backend uses SHA-1/SHA-2 0.11, whose runtime CPU detection selects
Rust intrinsics on supported ARM/x86 CPUs and a software fallback otherwise.
No C/assembly build dependency or target-specific compiler flags are required.
In particular, do not downgrade SHA-2 to 0.10 without checking ARM throughput:
its default backend does not select the ARM SHA instructions.

Compare throughput with
`cargo run -p librqbit-sha1-wrapper --release --example throughput --no-default-features --features sha1-rustcrypto`
and repeat with `sha1-ring` on the same machine.
