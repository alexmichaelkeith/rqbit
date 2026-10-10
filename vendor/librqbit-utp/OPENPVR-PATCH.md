# uTP dependency hardening

Source: crates.io librqbit-utp 0.7.0. Upgrade ringbuf to >=0.5.2 for
RUSTSEC-2026-0293. The torrent engine uses byte buffers, which do not have
panicking destructors, but no affected ringbuf release should remain in the
production graph. This patch belongs to the rqbit integration repository.
