FROM flink:1.18.1-scala_2.12-java11

USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip netcat-openbsd postgresql-client && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY flink_job.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh && \
    chown -R flink:flink /app

USER flink

CMD ["/app/entrypoint.sh"]