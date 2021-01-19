FROM gradle:6.6-jdk11 AS build
COPY ./ .
RUN gradle --no-daemon test dockerPrepare

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]
