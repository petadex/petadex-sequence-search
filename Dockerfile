FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies + Build tools for Biopython/NumPy and DIAMOND.
# gcc/gcc-c++/python3-devel are mandatory for Biopython; cmake/make/zlib-devel
# are needed to build DIAMOND from source (see below).
RUN yum install -y wget tar gzip gcc gcc-c++ python3-devel cmake make zlib-devel

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
# binaries — there is no prebuilt arm64/aarch64 download — so we build 2.1.11
# from source. Fast natively; slower under QEMU emulation in CI but fine.
ARG DIAMOND_VERSION=2.1.11
RUN wget -O diamond.tar.gz \
      https://github.com/bbuchfink/diamond/archive/refs/tags/v${DIAMOND_VERSION}.tar.gz && \
    tar xzf diamond.tar.gz && \
    cmake -S diamond-${DIAMOND_VERSION} -B diamond-build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build diamond-build -j "$(nproc)" && \
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