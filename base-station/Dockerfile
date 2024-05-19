# Description: Dockerfile for the base station application

# Use the Maven image as the base image
FROM maven:3.9.6-eclipse-temurin-21-alpine

# Set the working directory
WORKDIR /app

# Download and install Python and required packages
RUN apk add --no-cache python3 py3-pandas py3-elasticsearch py3-watchdog py3-arrow

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
