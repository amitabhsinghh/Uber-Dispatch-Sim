# ---- build stage (Debian/glibc) ----
FROM golang:1.22-bookworm AS build
WORKDIR /app

# Needed for confluent-kafka-go (CGo) and friends
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config librdkafka-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy module file first (better caching)
COPY go.mod ./
# Copy the rest
COPY . .

# Ensure deps/go.sum present
RUN go mod tidy

# Build with CGO so it links to system librdkafka
RUN CGO_ENABLED=1 go build -o dispatch

# ---- runtime stage (Debian/glibc) ----
FROM debian:bookworm-slim
WORKDIR /app

# Runtime deps for confluent-kafka-go
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /app/dispatch /app/dispatch

EXPOSE 9100
CMD ["/app/dispatch"]
