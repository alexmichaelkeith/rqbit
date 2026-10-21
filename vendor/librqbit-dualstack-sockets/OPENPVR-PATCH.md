# Windows VPN-bound sockets

Base: librqbit-dualstack-sockets 0.7.0, Apache-2.0.

Windows resolves adapter aliases through IP Helper, selects IP_UNICAST_IF in
network byte order, and explicitly binds every TCP/UDP/listen socket to the
adapter IPv4 address. IPv6 on an IPv4 VPN fails closed. HTTP tracker clients
also bind that source address and disable environment proxies. All errors
propagate; there is no unbound fallback. The OpenPVR Windows helper must keep
strong-host mode and a source-address egress filter for the adapter lifetime.

This vendored library is owned by the rqbit fork so all engine subsystems and
OpenPVR DNS use identical socket types and policies.
