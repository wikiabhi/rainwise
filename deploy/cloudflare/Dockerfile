# Build context is the repository root (see image_build_context in wrangler.jsonc).
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rainwise ./cmd/rainwise

FROM alpine:3.20
ARG LITESTREAM_VERSION=0.3.13
ARG TARGETARCH=amd64
RUN apk add --no-cache ca-certificates \
 && wget -qO /tmp/ls.tgz "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-v${LITESTREAM_VERSION}-linux-${TARGETARCH}.tar.gz" \
 && tar -xzf /tmp/ls.tgz -C /usr/local/bin litestream \
 && rm /tmp/ls.tgz \
 && adduser -D -u 10001 rainwise \
 && mkdir -p /data && chown rainwise /data
COPY --from=build /out/rainwise /usr/local/bin/rainwise
COPY deploy/cloudflare/config.yaml /etc/rainwise/config.yaml
COPY deploy/cloudflare/litestream.yml /etc/litestream.yml
COPY deploy/cloudflare/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER rainwise
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
