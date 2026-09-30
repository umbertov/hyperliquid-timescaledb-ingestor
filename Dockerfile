FROM nixos/nix:2.35.2 AS build

WORKDIR /src
ENV NIX_CONFIG="experimental-features = nix-command flakes"

COPY . .
RUN nix build .#default --out-link /result

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates
COPY --from=build /result/bin/hyperliquid-timescaledb-collector /usr/local/bin/hyperliquid-timescaledb-collector

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/hyperliquid-timescaledb-collector"]
