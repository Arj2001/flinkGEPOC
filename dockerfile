#FROM flink:1.17.0-java11
#COPY ./target/flink-example-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar
#CMD ["./bin/flink", "run", "/opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar"]

FROM flink:1.17.0-java11
COPY ./target/flink-example-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.5 \
    python3-pip \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install net-tools
COPY app.py /opt/flink/usrlib/app.py
RUN pip3 install flask waitress
RUN sed '2i\nohup python3 /opt/flink/usrlib/app.py &' /docker-entrypoint.sh > /temp.sh && mv /temp.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

