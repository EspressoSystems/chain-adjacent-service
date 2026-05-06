FROM ghcr.io/foundry-rs/foundry:latest

USER root
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends socat curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/state && chown foundry:foundry /app/state

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER foundry

EXPOSE 8545 8546

ENTRYPOINT ["/app/entrypoint.sh"]
