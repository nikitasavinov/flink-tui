ARG FLINK_VERSION=2.3.0
ARG FLINK_IMAGE=flink:${FLINK_VERSION}-java17
FROM maven:3-eclipse-temurin-24 AS build

ARG FLINK_VERSION
WORKDIR /build
COPY pom.xml .
RUN case "$FLINK_VERSION" in 1.*) profile=flink-1 ;; *) profile=flink-2 ;; esac \
    && mvn -q -P"$profile" -Dflink.version="$FLINK_VERSION" -DskipTests dependency:go-offline
COPY src ./src
RUN case "$FLINK_VERSION" in 1.*) profile=flink-1 ;; *) profile=flink-2 ;; esac \
    && mvn -q -P"$profile" -Dflink.version="$FLINK_VERSION" -DskipTests package

FROM ${FLINK_IMAGE}
COPY --from=build /build/target/flink-tui-demo.jar /opt/flink/usrlib/flink-tui-demo.jar
