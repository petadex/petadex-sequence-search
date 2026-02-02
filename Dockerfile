FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies + Build tools for Biopython/NumPy
# Adding gcc and python3-devel is mandatory for Biopython
RUN yum install -y wget tar gzip gcc gcc-c++ python3-devel

# Install MMseqs2 (arm64 for Apple Silicon & AWS Graviton)
RUN wget https://mmseqs.com/latest/mmseqs-linux-arm64.tar.gz && \
    tar xvzf mmseqs-linux-arm64.tar.gz && \
    cp mmseqs/bin/* /usr/local/bin/ && \
    rm -rf mmseqs mmseqs-linux-arm64.tar.gz

# Verify MMseqs2 installation
RUN mmseqs version

# Install Python dependencies
COPY requirements.txt .
# Use --no-cache-dir to ensure we don't pull a broken build attempt
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY cli.py ${LAMBDA_TASK_ROOT}

# Make cli.py executable
RUN chmod +x ${LAMBDA_TASK_ROOT}/cli.py

# Default: Lambda mode
# Override with: docker run --entrypoint python3 image cli.py
CMD ["lambda_function.handler"]