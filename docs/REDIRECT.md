# Redirect: NiFi talks to the simulator, not to the real exchanges

NiFi keeps its real URLs (for example `wss://ws.okx.com:8443/ws/v5/public`). Three things
make those calls reach the simulator instead:

1. **DNS.** Inside lpa-core's docker network, the exchange host names point to the
   simulator's container. This catches the calls NiFi makes directly.
2. **Proxy.** Some calls go through an HTTP proxy (okx WebSocket and REST, lbank, bybit
   REST). For a test, NiFi's proxy parameter points to the simulator, which acts as that
   proxy.
3. **TLS.** The simulator shows a certificate for those names. It is signed by our own
   test CA, and NiFi trusts that CA.

NiFi's flows do not change. Only one NiFi parameter changes during a test (step 5).
lpa-console subscribes markets as usual.

> ⚠ **Status (2026-10-21): written, NOT yet run against NiFi.** Steps 3 and 5 come from
> reading the NiFi flow (`NIFI-Version-Controller`). The DNS, proxy and CA steps were
> checked only with test clients (Go, curl), not with NiFi.

## While the redirect is on

- **Every** NiFi call to a simulated host name reaches the simulator. NiFi cannot reach the
  real exchange behind that name.
- A **stopped** exchange in the simulator is **silent**. It keeps the connection and answers
  subscribe and ping, but sends no market data.
- While NiFi's proxy points to the simulator, a proxied call for a host the simulator does
  not play yet (for example lbank) is **refused**. It is not sent to the real proxy.
- Stop the real exchange feeds you do not want to mix with test data.

## Steps

### 1. Make the certificates (once)

```sh
cp .env.example .env          # first time only; edit if needed
go run . -gen-certs           # or: docker compose -f deploy/docker-compose.yml run --rm exchange-simulator -gen-certs
```

This writes `certs/ca.crt`, `certs/ca.key` (the test CA) and `certs/server.crt`,
`certs/server.key` (the certificate for every host name in `.env`).

- **Keep `certs/`.** NiFi trusts this CA. A new CA means doing step 3 again.
- **Never commit `certs/`.** Anyone with `ca.key` can fake any host name for NiFi.
  It is in `.gitignore`.
- The server certificate is made again on every start. A new exchange in `.env` needs
  no new trust step.

### 2. Start the simulator inside lpa-core's network

```sh
docker compose -f deploy/docker-compose.yml --env-file .env up -d --build
```

The container joins `lpa-core_data-collector-net` (change it with `LPA_NETWORK`). It has
the exchange host names as **network aliases**: Docker's DNS answers those names with this
container's address, for every container on that network. Check it from NiFi:

```sh
docker exec nifi getent hosts ws.okx.com      # must print the simulator's address
```

### 3. Make NiFi trust the test CA

The NiFi flow has no SSL Context Service (not on the WebSocket clients, not on InvokeHTTP),
so NiFi uses Java's default truststore, `cacerts`:

```sh
docker cp certs/ca.crt nifi:/tmp/exchange-simulator-ca.crt
docker exec -u root nifi keytool -importcert -noprompt -cacerts -storepass changeit \
    -alias exchange-simulator-ca -file /tmp/exchange-simulator-ca.crt
docker restart nifi
```

⚠ This change lives in the container, not in the image: a recreated NiFi container
loses it, and the step must be done again.

If someone later adds an SSL Context Service with its own truststore file, import the same
`ca.crt` into that truststore instead, then disable and enable the service.

### 4. Make NiFi connect again

NiFi keeps open connections, and Java caches DNS answers (about 30 s by default). Stop and
start the exchange groups' `ConnectWebSocket` processors.

⚠ **Use Stop, never Terminate** on `ConnectWebSocket`: after a Terminate it keeps a broken
session factory until NiFi restarts (see `memory/project_nifi_wallex_ws.md`).

### 5. Point NiFi's proxy to the simulator

These calls go through one HTTP proxy (NiFi parameters `proxy_server.host` and
`proxy_server.port`): okx WebSocket and REST, lbank WebSocket and REST, bybit REST. That
proxy is on another machine and looks the host name up itself, so step 2 does not catch
them.

The simulator is also an HTTP proxy, on the **same port** as the real one (`PROXY_PORT`).
So for a test, change only the host:

1. In NiFi, open the parameter context `CONFIG`. **Write down the current value of
   `proxy_server.host`.**
2. Set `proxy_server.host` to `exchange-simulator` (the container name). Leave
   `proxy_server.port` as it is. NiFi restarts the processors and services that use it.

The simulator tunnels a `CONNECT` for a host and port it plays (for example
`ws.okx.com:8443`) to its own fake exchange. It refuses every other host, and the
simulator's log says `proxy refused a host the simulator does not play` for each one.

### 6. Check

```sh
curl -s localhost:9501/status          # each exchange: running, connections, markets
```

Subscribe a test market in lpa-console. Its exchange's `connections` and `markets` must go
up, and its books must show in orderbook-viewer.

## Undo

1. In NiFi, set `proxy_server.host` back to the value you wrote down in step 5.
2. `docker compose -f deploy/docker-compose.yml down` — the aliases go with the container,
   so the names resolve to the real exchanges again.
3. Stop and start NiFi's `ConnectWebSocket` processors (after ~30 s for the DNS cache).
4. Optional: remove the CA from NiFi
   (`keytool -delete -cacerts -storepass changeit -alias exchange-simulator-ca`, then
   restart NiFi).
