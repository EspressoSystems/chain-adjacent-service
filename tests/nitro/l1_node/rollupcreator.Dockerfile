FROM --platform=linux/amd64 node:20-trixie-slim
RUN apt-get update && \
    apt-get install -y git docker.io python3 make gcc g++ curl jq
WORKDIR /workspace
RUN git clone --no-checkout https://github.com/EspressoSystems/nitro-contracts.git ./
RUN git checkout integrate-cas
RUN git submodule update --init --recursive
RUN yarn install && yarn cache clean
RUN curl -L https://foundry.paradigm.xyz | bash
ENV PATH="${PATH}:/root/.foundry/bin"
RUN foundryup --install 1.0.0
# Hardhat needs aws-nitro-enclave-attestation resolvable as an npm package
RUN ln -s /workspace/lib/espresso-tee-contracts/lib/aws-nitro-enclave-attestation/contracts/src \
        /workspace/node_modules/aws-nitro-enclave-attestation && \
    echo '{"name":"aws-nitro-enclave-attestation","version":"0.0.0"}' \
        > /workspace/node_modules/aws-nitro-enclave-attestation/package.json
RUN touch scripts/config.ts
RUN yarn build:all
ENTRYPOINT ["yarn"]
