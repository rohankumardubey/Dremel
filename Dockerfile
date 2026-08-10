FROM rust:1.97.1-bookworm

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates cmake curl gnupg ninja-build python3 lsb-release software-properties-common \
    && curl -fsSL https://apt.llvm.org/llvm.sh -o /tmp/llvm.sh \
    && chmod +x /tmp/llvm.sh \
    && /tmp/llvm.sh 22 \
    && apt-get install -y --no-install-recommends libc++-22-dev libc++abi-22-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.sh

ENV CC=clang-22 CXX=clang++-22 CPP_STANDARD=26
WORKDIR /workspace
COPY . .

RUN python3 scripts/create_workload.py \
    && python3 scripts/create_extended_workload.py \
    && cargo test --manifest-path dremel-rs/Cargo.toml \
    && cmake -S dremel-cpp -B dremel-cpp/build -G Ninja \
         -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++-22 \
         -DDREMEL_CXX_STANDARD=26 \
    && cmake --build dremel-cpp/build \
    && ctest --test-dir dremel-cpp/build --output-on-failure

CMD ["bash", "-lc", "DATASET_ROWS=${DATASET_ROWS:-1000000} WARMUP=${WARMUP:-3} ITERATIONS=${ITERATIONS:-20} ./benchmark.sh"]
