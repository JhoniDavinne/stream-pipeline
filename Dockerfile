FROM apache/spark:3.5.3-python3

USER root
WORKDIR /opt/stream-pipeline

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY producer ./producer
COPY data ./data

ENV PYTHONPATH=/opt/stream-pipeline/src
ENV PIPELINE_BASE_DIR=/opt/stream-pipeline

CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/opt/stream-pipeline/src/pipeline.py"]
