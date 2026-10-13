# OpenPVR integration fork

Local experiment: `openpvr/explicit-piece-demand-local` simplifies the published
integration below. It is not pushed. rqbit executes one exact ordered piece
queue and exposes background completion. OpenPVR owns read windows, consumer
budgets, fairness, deduplication and demand leases. The engine contains no
viewer IDs or per-reader priority controls. Ordinary rqbit readers retain
upstream lookahead when no caller-controlled queue is supplied.
OpenPVR's `examples/local_torrent_seed.rs` provides the loopback-only seeder
and tracker used for playback checks; the fixture stays with the application.

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
- **Piece inspection:** exported `FileStream`, piece indices and
  `PieceSnapshot` support embedding and diagnostics. The snapshot supplies
  verified, queued and in-flight states with piece length in one read.
  OpenPVR uses upstream `Api::api_dump_haves` for compact bitfields and
  `TorrentStorage::on_piece_completed` for notifications. Requested-piece
  highlighting belongs to OpenPVR; the fork has no custom bitmap accessor,
  notification channel or completion subscription API.
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

## Preparing upstream contributions

Prepare independent patches for uTP dependency hardening, provider-free TLS,
the RustCrypto hashing backend, live announce-port updates, generic DNS
injection, piece inspection and caller-controlled piece scheduling. Preserve
upstream defaults and keep application fixtures and this fork's documentation
out of those patches. Discuss scheduling API shape before proposing it.

Coordinate piece inspection with existing upstream bitfield/progress work
(PRs #574 and #650). A compact bitfield API and storage completion callback
already exist; do not add competing APIs for those capabilities. Include
default-feature and supported-platform checks in addition to OpenPVR's
provider-free configuration. Local Mac/OpenPVR checks alone do not qualify
every upstream binary, desktop app or feature combination.

Upstream requires AI-assistance disclosure and human review/understanding;
see https://github.com/ikatson/rqbit/blob/main/AI_POLICY.md. No upstream
submission or remote push is part of this local experiment.

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
