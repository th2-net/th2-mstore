FROM gradle:6.6-jdk11 AS build
ARG release_version
COPY ./ .
RUN gradle --no-daemon clean build dockerPrepare -Prelease_version=${release_version}

FROM adoptopenjdk/openjdk11:alpine

# FIXME: remove when release
RUN apt-get update
RUN apt-get install -y ifstat

WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]
