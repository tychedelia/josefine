FROM rust:latest

WORKDIR /opt/josefine

COPY . .

RUN cargo build --release

FROM scratch

WORKDIR /opt/josefine

COPY --from=0 /opt/josefine/target/release/josefine .
COPY ./Config.toml /etc/josefine/Config.toml

ENV JOSEFINE_CONFIG=/etc/josefine/Config.toml

CMD ["./josefine"]
