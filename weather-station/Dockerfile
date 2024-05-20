# docker file of weather station

# Use the Maven image as the base image
FROM maven:3.9.6-eclipse-temurin-21-alpine

# Set the working directory
WORKDIR /app

# Copy the pom.xml file to the working directory
COPY pom.xml /app

# Install the dependencies
RUN mvn dependency:go-offline

# Copy the source code to the working directory
COPY src /app/src

# Build the project
RUN mvn clean package

# Run the application
CMD ["mvn", "exec:java"]