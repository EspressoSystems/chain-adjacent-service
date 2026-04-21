FROM ghcr.io/foundry-rs/foundry:latest

USER root
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends socat \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER foundry

EXPOSE 8545 8546

ENTRYPOINT ["/app/entrypoint.sh"]
