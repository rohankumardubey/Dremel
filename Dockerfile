FROM rust:1.97.1-trixie AS rust-toolchain

FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
ADD https://apt.llvm.org/llvm-snapshot.gpg.key /tmp/llvm-snapshot.gpg.key
ADD https://packages.apache.org/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb /tmp/apache-arrow-apt-source.deb
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates cmake gnupg ninja-build python3 python3-venv
RUN gpg --dearmor --output /usr/share/keyrings/llvm-snapshot.gpg \
      /tmp/llvm-snapshot.gpg.key \
    && echo "deb [signed-by=/usr/share/keyrings/llvm-snapshot.gpg] https://apt.llvm.org/noble/ llvm-toolchain-noble-22 main" \
      | tee /etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends clang-22 \
    && apt-get install -y --no-install-recommends libc++-22-dev libc++abi-22-dev
RUN apt-get install -y /tmp/apache-arrow-apt-source.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends libarrow-dev libparquet-dev

ENV PATH=/usr/local/cargo/bin:$PATH RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo CC=clang-22 CXX=clang++-22 CPP_STANDARD=26
WORKDIR /workspace
COPY . .

RUN python3 -m venv .venv \
    && .venv/bin/pip install -r requirements.txt \
    && .venv/bin/python scripts/create_workload.py \
    && .venv/bin/python scripts/create_extended_workload.py \
    && cargo test --manifest-path dremel-rs/Cargo.toml \
    && cmake -S dremel-cpp -B dremel-cpp/build -G Ninja \
         -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++-22 \
         -DDREMEL_CXX_STANDARD=26 \
    && cmake --build dremel-cpp/build \
    && ctest --test-dir dremel-cpp/build --output-on-failure

CMD ["bash", "-lc", "DATASET_ROWS=${DATASET_ROWS:-1000000} WARMUP=${WARMUP:-3} ITERATIONS=${ITERATIONS:-20} ./benchmark.sh"]
