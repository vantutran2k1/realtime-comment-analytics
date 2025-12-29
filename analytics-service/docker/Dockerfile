FROM apache/spark:3.5.0-python3
USER root
# Đảm bảo có torch và transformers
RUN pip install --no-cache-dir torch transformers pandas \
    psycopg2-binary numpy==1.24.3 pyarrow kafka-python
USER 185