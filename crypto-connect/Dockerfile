FROM amazoncorretto:8-alpine
WORKDIR /app
ADD target/crypto-connect-1.0-SNAPSHOT-shaded.jar /app/crypto-connect-1.0-SNAPSHOT-shaded.jar
ADD config/dev /app/config
ENTRYPOINT ["/usr/bin/java", "-jar", "/app/crypto-connect-1.0-SNAPSHOT-shaded.jar"]