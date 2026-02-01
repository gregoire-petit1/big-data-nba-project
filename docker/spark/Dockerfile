FROM apache/spark:3.5.8-python3

USER root
RUN pip install --no-cache-dir pandas==2.2.1 requests==2.32.3 boto3==1.34.42 s3fs==2024.3.1
ENV PYTHONPATH="/opt/spark/jobs"
USER root
