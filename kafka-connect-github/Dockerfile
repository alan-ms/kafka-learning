FROM lensesio/fast-data-dev:3.6.1

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .
COPY src ./src

RUN ./mvnw clean package

COPY ./target/kafka-connect-github-1.0-SNAPSHOT-package/share/java/kafka-connect-github ./connectors

ENV ADV_HOST=127.0.0.1
ENV RUNTESTS=0

EXPOSE 3030
EXPOSE 2181
EXPOSE 8081
EXPOSE 8083
EXPOSE 9092
