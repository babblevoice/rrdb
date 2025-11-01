# syntax=docker/dockerfile:1.7

ARG ALPINE_VERSION=3.22

# ===== builder =====
FROM alpine:${ALPINE_VERSION} AS builder
WORKDIR /usr/src/rrdb

# Build deps only in this stage
RUN apk add --no-cache alpine-sdk make

# Bring in source
COPY . .

# Build rrdb
RUN make clean && make && make install


# ===== dev (fat, for hacking & tests) =====
FROM alpine:${ALPINE_VERSION} AS dev
LABEL org.opencontainers.image.title="rrdb-dev" \
      org.opencontainers.image.description="Dev/Test image with Node, Mocha, faketime"

# Workspace is volume-mounted at runtime
WORKDIR /workspace

COPY . .

# Tools you actually use while fixing things
# nodejs + npm for tests, libfaketime for time warping, build tools if you want to rebuild in-container
RUN apk add --no-cache \
      nodejs npm git \
      alpine-sdk make \
      libfaketime; \
      npm ci

# Copy built binary from builder so tests can exec it directly
COPY --from=builder /usr/bin/rrdb /usr/bin/rrdb

# Helpful default command: show versions, then keep shell
CMD [ "bash", "-lc", "node -v && npm -v && mocha --version || true; exec bash" ]

# ===== app (lean runtime) =====
FROM alpine:${ALPINE_VERSION} AS app

LABEL org.opencontainers.image.title="rrdb" \
      org.opencontainers.image.description="Round-robin DB server" \
      org.opencontainers.image.source="local"

# runtime deps only
RUN apk add --no-cache ucspi-tcp6

# install built binary
COPY --from=builder /usr/bin/rrdb /usr/bin/rrdb

EXPOSE 13900
WORKDIR /var/rrdb
CMD [ "/usr/bin/tcpserver", "0", "13900", "/usr/bin/rrdb", "--command=-", "--dir=/var/rrdb" ]
