# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pulse ./cmd/pulse

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tini
COPY --from=build /out/pulse /usr/local/bin/pulse
EXPOSE 8741
ENV PULSE_ADDR=:8741
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["pulse", "server", "--addr", ":8741"]
