FROM gradle:6.6-jdk11 AS build
ARG release_version
COPY ./ .
RUN gradle test dockerPrepare -Prelease_version=${release_version}

FROM openjdk:12-alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]
