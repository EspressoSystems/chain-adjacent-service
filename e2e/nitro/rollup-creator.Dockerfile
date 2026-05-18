FROM node:18-slim

RUN apt-get update && apt-get install -y git python3 make g++ curl && rm -rf /var/lib/apt/lists/*

RUN curl -L https://foundry.paradigm.xyz | bash && /root/.foundry/bin/foundryup
ENV PATH="/root/.foundry/bin:${PATH}"

ARG NITRO_CONTRACTS_REPO=https://github.com/EspressoSystems/nitro-contracts.git
ARG NITRO_CONTRACTS_REF=jh/cas-2.1.3-contracts

WORKDIR /nitro-contracts
RUN git clone --depth 1 --branch ${NITRO_CONTRACTS_REF} ${NITRO_CONTRACTS_REPO} . \
    && git submodule update --init --recursive --depth 1

RUN yarn install --frozen-lockfile
RUN cp scripts/config.ts.example scripts/config.ts
RUN yarn build:all

ENTRYPOINT ["yarn", "create-rollup-testnode"]
