FROM apache/spark:3.5.3-python3

USER root

WORKDIR /opt/stream-pipeline

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY producer ./producer
COPY tests ./tests
COPY data ./data

ENV PYTHONPATH=/opt/stream-pipeline
ENV PIPELINE_BASE_DIR=/opt/stream-pipeline

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/opt/stream-pipeline/src/medallion_pipeline.py"]