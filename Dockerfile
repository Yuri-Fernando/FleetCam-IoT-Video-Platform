FROM gradle:8.10.2-jdk17 AS build
WORKDIR /app
COPY settings.gradle.kts build.gradle.kts ./
COPY src ./src
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon bootJar
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/build/libs/fleetcam-0.1.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
