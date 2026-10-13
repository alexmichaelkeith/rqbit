# OpenPVR integration fork

Local experiment: `openpvr/explicit-piece-demand-local` simplifies the published
integration below. It is not pushed. rqbit executes one exact ordered piece
queue and exposes background completion. OpenPVR owns read windows, consumer
budgets, fairness, deduplication and demand leases. The engine contains no
viewer IDs or per-reader priority controls. Ordinary rqbit readers retain
upstream lookahead when no caller-controlled queue is supplied.
`examples/local_playback_seed.rs` provides a loopback-only seeder
and tracker for a generated video; it disables DHT and LAN discovery.

The `openpvr/upstream-20261012` integration branch includes upstream
`ikatson/rqbit` main through `57e95d16c830863a1fde32d8fa98153b53a3d0b4` (9.0.1), fetched on 2026-10-12.
OpenPVR's `Cargo.toml` selects the integration branch and its `Cargo.lock`
records the exact revision. Keep engine fixes here rather than patching
librqbit inline in OpenPVR; application policy belongs in OpenPVR.

## Changes still required by OpenPVR

- **Piece demand:** one caller-controlled ordered queue, wakeups for changed
  requests, and explicit background completion. `Some(empty)` requests no
  pieces; `None` restores upstream reader scheduling. OpenPVR owns all
  consumer coordination and releases demand on scope exit or cancellation.
- **Administrative progress:** exported `FileStream`, piece indices and
  `PieceSnapshot`/bitmap notifications support OpenPVR's admin diagnostics.
  The snapshot supplies availability, scheduling state and piece length in
  one read; the cheap bitmap accessor remains for availability checks.
- **DNS injection:** a generic `HostResolver` supplied by the embedding app
  is used for HTTP/UDP trackers and DHT bootstrap, without system-DNS fallback
  on errors. OpenPVR owns interface-bound DNS, its parser and validation.
  The standalone CLI retains upstream networking options; the fork's custom
  `--dns-server` option is retired. OpenPVR still refuses VPN startup without
  a verified interface and bound DNS server.
- **Port forwarding:** update the announced port while a session is running
  and wake tracker announcements when it changes. OpenPVR uses this when a
  forwarded VPN port changes. The watch channel is the sole port state and
  retains updates even before any tracker subscribes.
- **Independent crypto selection:** `rust-tls-no-provider` lets the embedding
  app install its Rustls provider, while `sha1-rustcrypto` selects RustCrypto
  hashing with CPU intrinsics independently of TLS.
- **uTP dependency patch:** the vendored 0.7.0 package still needs its
  `ringbuf >=0.5.2` change; see `vendor/librqbit-utp/OPENPVR-PATCH.md`.

## Retired custom changes

Upstream `513adb54` fixes the failed-handshake/backlog `select!` panic. The
fork's `ListenerEvent`/`next_listener_event` implementation has been removed;
the real-listener regression test exercises upstream's implementation.
Upstream's configurable handshake limit already replaces the old fixed cap.

The pre-9.0 `PersistentDhtConfig`, `disable_dht`, `dht_config`, and
`dht_bootstrap_addrs` compatibility fields are also removed. OpenPVR now uses
upstream `DhtSessionConfig` and `DhtPersistenceConfig` directly.

## Updating again

1. Fetch upstream main and create an integration branch from the current
   OpenPVR revision. Merge upstream without rewriting published history.
2. Review the diff against upstream, especially streaming initialization,
   piece scheduling, bound DNS, TLS features and listener lifecycle. Retire
   fixes that upstream now supplies; keep their useful regression coverage.
3. Preserve provider-free TLS when Reqwest changes feature names. Refresh the
   lockfile and run formatting plus the library, core, tracker and hashing
   tests with `--no-default-features --features
   librqbit/rust-tls-no-provider,librqbit/sha1-rustcrypto`.
4. Run OpenPVR's full tests and native dependency gates against the candidate.
   Push the tested integration branch, then update OpenPVR's git dependency
   and lockfile. Verify the final build resolves the published git revision,
   with no local path override.

The fork retains upstream's default feature choices for ordinary rqbit users.
