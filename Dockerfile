FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies + Build tools for Biopython/NumPy and DIAMOND.
# gcc/gcc-c++/python3-devel are mandatory for Biopython; cmake/make/zlib-devel
# are needed to build DIAMOND from source (see below).
# NOTE: cmake3 (3.17 on AL2), not cmake — the base image's default `cmake` is
# 2.8.12, too old for the `cmake -S/-B` out-of-source syntax below (needs ≥3.13).
# sqlite-devel: DIAMOND 2.2.x's blastdb.cpp needs sqlite3.h. libzstd-devel:
# enables WITH_ZSTD=ON so DIAMOND can read a `.fa.zst` compressed-FASTA DB (the
# DB-format streaming benchmark / "06" zstd arm).
RUN yum install -y wget tar gzip gcc gcc-c++ python3-devel cmake3 make zlib-devel \
      sqlite-devel libzstd-devel

# Install MMseqs2 (arm64 for Apple Silicon & AWS Graviton).
# KEPT during the DIAMOND transition — the legacy single-Lambda search path
# still uses it. Remove in Phase 7 cutover.
RUN wget https://mmseqs.com/latest/mmseqs-linux-arm64.tar.gz && \
    tar xvzf mmseqs-linux-arm64.tar.gz && \
    cp mmseqs/bin/* /usr/local/bin/ && \
    rm -rf mmseqs mmseqs-linux-arm64.tar.gz

# Verify MMseqs2 installation
RUN mmseqs version

# Install DIAMOND (sharded scale-out engine). DIAMOND ships only x86-64 release
# binaries — there is no prebuilt arm64/aarch64 download — so we build from
# source. Fast natively; slower under QEMU emulation in CI but fine.
# v2.2.1 (bumped from 2.1.11): adds compressed-FASTA-as-DB read support; built
# WITH_ZSTD=ON so a `.fa.zst` shard works as `-d`. The `.dmnd` path is
# behaviorally identical to 2.1.11 (result-identity verified at deploy).
# -DZSTD_LIBRARY points at the shared lib explicitly — DIAMOND's CMake otherwise
# only auto-searches for the static `.a`.
ARG DIAMOND_VERSION=2.2.1
RUN wget -O diamond.tar.gz \
      https://github.com/bbuchfink/diamond/archive/refs/tags/v${DIAMOND_VERSION}.tar.gz && \
    tar xzf diamond.tar.gz && \
    cmake3 -S diamond-${DIAMOND_VERSION} -B diamond-build -DCMAKE_BUILD_TYPE=Release \
      -DWITH_ZSTD=ON -DZSTD_LIBRARY=/usr/lib64/libzstd.so && \
    cmake3 --build diamond-build -j "$(nproc)" && \
    cp diamond-build/diamond /usr/local/bin/ && \
    rm -rf diamond.tar.gz diamond-${DIAMOND_VERSION} diamond-build

# Verify DIAMOND installation
RUN diamond version

# Install Python dependencies
COPY requirements.txt .
# Use --no-cache-dir to ensure we don't pull a broken build attempt
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY common.py ${LAMBDA_TASK_ROOT}
COPY worker.py ${LAMBDA_TASK_ROOT}
COPY orchestrator.py ${LAMBDA_TASK_ROOT}
COPY aggregator.py ${LAMBDA_TASK_ROOT}
COPY cli.py ${LAMBDA_TASK_ROOT}

# Make cli.py executable
RUN chmod +x ${LAMBDA_TASK_ROOT}/cli.py

# Default handler: the legacy MMseqs2 search Lambda. One image serves multiple
# roles — the DIAMOND worker Lambda overrides this CMD to `worker.handler`
# (and the future orchestrator to its own handler) via function config.
# Override with: docker run --entrypoint python3 image cli.py
CMD ["lambda_function.handler"]