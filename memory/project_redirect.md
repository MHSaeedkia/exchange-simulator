---
name: redirect
description: How NiFi's real-URL calls reach the simulator (docker network aliases + own test CA), why aliases and not extra_hosts, what is verified and what is still open (NiFi uses JVM cacerts; proxied calls — okx, lbank, bybit REST — reach the simulator's own HTTP proxy: a test changes only NiFi's proxy_server.host)
metadata:
  type: project
---

# The redirect (step 1, 2026-10-21)

User decision: **DNS + our own test CA, NiFi flows unchanged** ([[project-scope]]).
Procedure for people: `docs/REDIRECT.md`.

## How it is built, and why

- **DNS = docker network aliases on the simulator's container**
  (`deploy/docker-compose.yml`), not `extra_hosts` on NiFi. The plan said extra_hosts; aliases
  were chosen while building because they need NO change to lpa-core's compose or NiFi's
  container, need no fixed IP, and undo themselves: stop the simulator and the names resolve
  to the real exchanges again. Every host name of every exchange must be listed under
  `aliases` (one line per exchange step, `${<EX>_WS_HOST:-default}`).
- **TLS = `internal/certs`.** The CA is made only when its files are missing (a new CA would
  silently break NiFi's trust; the start log WARNs when one is made). The server certificate
  (RSA 2048, every host name as a SAN) is made again on every start from `config.Hosts()`,
  so a new exchange needs no new trust step. `certs/` is gitignored and must be a volume.
- **One listener per port; routing by Host + path** (`internal/server`), so many exchanges
  can share 443. Two exchanges claiming the same host + path is a startup error.

## Verified

- Unit test: a client that trusts the CA dials `wss://ws.okx.com:8443/…` with the address
  forced to the simulator → TLS verifies for the real name; a client without the CA fails.
- Live, local binary (2026-10-21): same with the tlsclient in the scratchpad and with
  `curl --resolve www.okx.com:…:127.0.0.1 --cacert certs/ca.crt` for REST.

## NOT verified / open

- **The docker alias trick with NiFi** (Docker was down): `docker exec nifi getent hosts
  ws.okx.com` must print the simulator's address. Believed to work (Docker's embedded DNS
  answers aliases on a user-defined network before forwarding) — check it.
- **Trusted CAs — answered from the flow (2026-10-21):** no SSLContextService anywhere, so
  NiFi uses Java's default `cacerts` (step 3 of REDIRECT.md = keytool into `cacerts`; likely,
  not yet tried on NiFi). lpa-core's NiFi image keeps `cacerts` in the image, not on a volume,
  so a recreated `nifi` container loses our CA *(inferred)*.
- **The proxy — confirmed from the flow, solved by user choice (2026-10-21), see below.**
- Java caches DNS (~30 s): NiFi's ConnectWebSocket must be stopped/started after switching.
  Never Terminate it ([[project-nifi-wallex-ws]]).

## The proxy (user decision 2026-10-21)

okx WS **and** REST (also lbank WS + REST, bybit REST) go through one HTTP proxy on another
machine (NiFi parameters `proxy_server.host/port`). It resolves the name itself, so DNS
aliases never see these calls. Options given: (A) the simulator is also the proxy, (B) empty
the proxy parameters. **User chose A**, with the extra wish "for a test I change only ONE
parameter". They first said "only the port" — impossible, the host is the other machine; so:
**`PROXY_PORT` defaults to the real proxy's port and a test changes only
`proxy_server.host` → `exchange-simulator`.** User agreed.

- CONNECT to a host:port the simulator serves → the hijacked conn is handed IN PROCESS to
  that port's listener (`portListener`), so TLS + Host routing run as for a direct call and
  there is no extra loopback hop on the hot path.
- Anything else → 403 (plain-http proxying → 405). **Never forwarded to the real proxy**
  (no pass-through, same rule as "stopped = silent"). So during a test lbank and bybit
  REST fail until they are simulated; forwarding them upstream would be a later, asked-for step.
- Verified: unit tests (gorilla WS + net/http through the proxy, refusals) and live with the
  local binary: WS through the proxy at 1000/s (3000 updates in 3 s, missed 0), NiFi's exact
  REST URL with curl `-x`, `api.lbkex.com` refused with 403, a client without our CA refused.
- NOT verified: NiFi's own clients (Jetty WS client, InvokeHTTP/OkHttp) through it; both
  use plain HTTP CONNECT with no proxy auth in the flow, so they are expected to work.

## Gotcha

- macOS lets a normal user bind ports below 1024 only on all interfaces (`0.0.0.0`), not on
  `127.0.0.1` → "bind: permission denied" on 443. In Docker this does not happen. Locally,
  move the port (`OKX_REST_PORT=9443`) or listen on 0.0.0.0.
