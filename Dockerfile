FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies
RUN yum install -y wget tar gzip

# Install MMseqs2
RUN wget https://mmseqs.com/latest/mmseqs-linux-avx2.tar.gz && \
    tar xvzf mmseqs-linux-avx2.tar.gz && \
    cp mmseqs/bin/* /usr/local/bin/ && \
    rm -rf mmseqs mmseqs-linux-avx2.tar.gz

# Verify MMseqs2 installation
RUN mmseqs version

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy Lambda function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD ["lambda_function.handler"]
