# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rainwise ./cmd/rainwise
# Distroless has no shell, so the data directory is made here and copied with
# its owner. A named volume mounted at /data inherits that ownership.
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rainwise /rainwise
COPY --from=build --chown=nonroot:nonroot /out/data /data
# A default so the image starts with its own config. Mount yours over it.
COPY examples/config.yaml /etc/rainwise/config.yaml
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/rainwise"]
CMD ["serve", "--config", "/etc/rainwise/config.yaml"]
