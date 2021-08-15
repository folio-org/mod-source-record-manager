FROM folioci/alpine-jre-openjdk11:latest

ENV VERTICLE_FILE mod-source-record-manager-server-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

# Copy your fat jar to the container
COPY mod-source-record-manager-server/target/${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}

ENV LIB_DIR ${VERTICLE_HOME}/lib

RUN mkdir -p ${LIB_DIR}

COPY mod-source-record-manager-server/lib/mod-source-record-storage-client-5.2.1-SNAPSHOT.jar ${LIB_DIR}/

# Expose this port locally in the container.
EXPOSE 8081
