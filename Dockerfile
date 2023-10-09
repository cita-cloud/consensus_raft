FROM rust:slim-bullseye AS buildstage
WORKDIR /build
ENV PROTOC_NO_VENDOR 1
RUN rustup component add rustfmt && \
    apt-get update && \
    apt-get install -y --no-install-recommends wget protobuf-compiler build-essential g++ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY . /build/
RUN cargo build --release

FROM debian:bullseye-slim
RUN useradd -m chain
USER chain
COPY --from=buildstage /build/target/release/consensus /usr/bin/
COPY --from=ghcr.io/grpc-ecosystem/grpc-health-probe:v0.4.19 /ko-app/grpc-health-probe /usr/bin/
CMD ["consensus"]
