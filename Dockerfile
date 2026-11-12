# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pulsar ./cmd/pulsar

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tini
COPY --from=build /out/pulsar /usr/local/bin/pulsar
EXPOSE 8741
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["pulsar", "server", "--addr", ":8741"]
