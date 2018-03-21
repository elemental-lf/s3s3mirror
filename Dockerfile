FROM maven:3.5.3-jdk-8 AS build

ENV GIT_TREEISH 0.5

RUN git clone --branch $GIT_TREEISH --depth 1 https://github.com/elemental-lf/s3s3mirror.git /build
WORKDIR /build

RUN mvn -Dmaven.test.skip=true package

FROM openjdk:8u151-jre-alpine3.7 AS runtime

RUN mkdir -p /run/target/conf && \
	apk add --no-cache bash ca-certificates 

COPY --from=build /build/s3s3mirror.sh /run/
COPY --from=build /build/target/s3s3mirror-1.2.6-SNAPSHOT.jar /run/target/
COPY --from=build /build/target/classes/log4j.xml /run/target/conf/
WORKDIR /run

ENTRYPOINT ["./s3s3mirror.sh"]
