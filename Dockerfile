# syntax=docker/dockerfile:1
FROM rust:1.97-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && cp target/release/tba /tba

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/alexandernicholson/tba" \
      org.opencontainers.image.description="Kubernetes time based autoscaler" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /tba /usr/local/bin/tba
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/tba"]
CMD ["run"]
