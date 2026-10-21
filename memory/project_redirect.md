---
name: redirect
description: How NiFi's real-URL calls reach the simulator (docker network aliases + own test CA), why aliases and not extra_hosts, what is verified and what is still open (NiFi truststore, okx REST proxy)
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
- **Where NiFi keeps trusted CAs** (SSLContextService or Java's cacerts) — from the exports.
- **okx REST goes through a proxy on another machine**, which resolves `www.okx.com` itself,
  so DNS does not catch it. Likely fix: the simulator also acts as that proxy — from the okx
  export.
- Java caches DNS (~30 s): NiFi's ConnectWebSocket must be stopped/started after switching.
  Never Terminate it ([[project-nifi-wallex-ws]]).

## Gotcha

- macOS lets a normal user bind ports below 1024 only on all interfaces (`0.0.0.0`), not on
  `127.0.0.1` → "bind: permission denied" on 443. In Docker this does not happen. Locally,
  move the port (`OKX_REST_PORT=9443`) or listen on 0.0.0.0.
