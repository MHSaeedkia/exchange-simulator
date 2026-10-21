# syntax=docker/dockerfile:1

# --- test stage ---
# Dependencies are vendored, so the image builds fully offline — no module proxy needed.
FROM golang:1.26-alpine AS test
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go test -mod=vendor ./...

# --- build stage ---
# Chained from test so BuildKit is forced to run the test stage.
FROM test AS build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -mod=vendor -o /exchange-simulator .

# --- runtime stage ---
# Runs as root on purpose: it listens on the exchanges' real ports (443) and reads Docker
# stats through the mounted docker socket. certs/ must be a volume: NiFi trusts the CA in it,
# and a new CA would break that trust. See deploy/docker-compose.yml.
FROM alpine:3.22
WORKDIR /app
COPY --from=build /exchange-simulator /app/exchange-simulator
COPY queries.example.txt /app/queries.example.txt

EXPOSE 443 8443 2053 9501
ENTRYPOINT ["/app/exchange-simulator"]
