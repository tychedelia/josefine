FROM rust:latest

WORKDIR /opt/josefine

COPY . .

RUN cargo build --release

FROM scratch

WORKDIR /opt/josefine

COPY --from=0 /opt/josefine/target/release/josefine .

CMD josefine