# Redirect: NiFi talks to the simulator, not to the real exchanges

NiFi keeps its real URLs (for example `wss://ws.okx.com:8443/ws/v5/public`). Two things
make those calls reach the simulator instead:

1. **DNS.** Inside lpa-core's docker network, the exchange host names point to the
   simulator's container.
2. **TLS.** The simulator shows a certificate for those names. It is signed by our own
   test CA, and NiFi trusts that CA.

NiFi's flows do not change. lpa-console subscribes markets as usual.

> ⚠ **Status (2026-10-21): written, NOT yet run against NiFi.** Two parts are still open:
> where NiFi keeps its trusted CAs (step 3), and how the okx REST proxy is set up (step 5).
> Both will be settled from the NiFi processor-group exports. The DNS step (network alias)
> and the CA step were checked only with a test client, not with NiFi.

## While the redirect is on

- **Every** NiFi call to a simulated host name reaches the simulator. NiFi cannot reach the
  real exchange behind that name.
- A **stopped** exchange in the simulator is **silent**. It keeps the connection and answers
  subscribe and ping, but sends no market data.
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

**Which way applies is not known yet** — it comes from the NiFi exports.

- **NiFi uses Java's default truststore** (no SSL Context Service on the WebSocket client
  or on InvokeHTTP):

  ```sh
  docker cp certs/ca.crt nifi:/tmp/exchange-simulator-ca.crt
  docker exec -u root nifi keytool -importcert -noprompt -cacerts -storepass changeit \
      -alias exchange-simulator-ca -file /tmp/exchange-simulator-ca.crt
  docker restart nifi
  ```

  ⚠ This change lives in the container, not in the image: a recreated NiFi container
  loses it, and the step must be done again.

- **NiFi uses an SSL Context Service with its own truststore file:** import the same file
  into that truststore (`keytool -importcert -keystore <truststore> -storepass <password>
  -alias exchange-simulator-ca -file ca.crt`), then disable and enable the service.

### 4. Make NiFi connect again

NiFi keeps open connections, and Java caches DNS answers (about 30 s by default). Stop and
start the exchange groups' `ConnectWebSocket` processors.

⚠ **Use Stop, never Terminate** on `ConnectWebSocket`: after a Terminate it keeps a broken
session factory until NiFi restarts (see `memory/project_nifi_wallex_ws.md`).

### 5. REST calls through a proxy (okx)

okx's REST snapshot goes through an HTTP proxy, because `www.okx.com` is blocked from the
server. A proxy on another machine looks up `www.okx.com` itself, so the DNS step does not
catch it. **How this is solved depends on the proxy setup in the okx export — open.**

### 6. Check

```sh
curl -s localhost:9501/status          # each exchange: running, connections, markets
```

Subscribe a test market in lpa-console. Its exchange's `connections` and `markets` must go
up, and its books must show in orderbook-viewer.

## Undo

1. `docker compose -f deploy/docker-compose.yml down` — the aliases go with the container,
   so the names resolve to the real exchanges again.
2. Stop and start NiFi's `ConnectWebSocket` processors (after ~30 s for the DNS cache).
3. Optional: remove the CA from NiFi
   (`keytool -delete -cacerts -storepass changeit -alias exchange-simulator-ca`, then
   restart NiFi).
