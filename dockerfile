FROM flink:1.17.0-java11
COPY ./target/flink-example-1.0-SNAPSHOT.jar /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar
CMD ["./bin/flink", "run", "/opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar"]