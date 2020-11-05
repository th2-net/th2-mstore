FROM gradle:6.6-jdk11 AS build
ARG release_version
COPY ./ .
RUN gradle dockerPrepare -Prelease_version=${release_version}

FROM openjdk:12-alpine
ENV CRADLE_INSTANCE_NAME=instance1 \
    CASSANDRA_DATA_CENTER=kos \
    CASSANDRA_HOST=cassandra \
    CASSANDRA_PORT=9042 \
    CASSANDRA_KEYSPACE=demo \
    CASSANDRA_USERNAME=guest \
    CASSANDRA_PASSWORD=guest \
    RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER="" \
    RABBITMQ_PASS="" \
    RABBITMQ_VHOST=th2 \
    RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY="" \
    TH2_CONNECTIVITY_ADDRESSES=""
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]
