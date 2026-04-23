FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev gcc curl

WORKDIR /src
COPY . .
RUN cargo build --release

FROM alpine:3

COPY --from=builder /src/target/release/stats-cruncher /usr/local/bin/stats-cruncher

EXPOSE 8080
CMD ["stats-cruncher", "--config", "/config.toml"]
