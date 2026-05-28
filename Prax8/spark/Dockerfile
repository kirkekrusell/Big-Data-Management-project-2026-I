FROM bitnamilegacy/spark:3.5

USER root

ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG JARS_DIR=/opt/bitnami/spark/jars

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# uid 1001 has no /etc/passwd entry, which breaks Hadoop's UserGroupInformation
# (NPE on null username) and Java's user.home lookup. Adding a real entry.
RUN echo 'spark:x:1001:0:spark:/tmp:/usr/sbin/nologin' >> /etc/passwd

# Iceberg + Hadoop-AWS for Spark <-> HMS <-> MinIO. Iceberg's spark-runtime
# bundles a shaded Hive metastore client so we don't need separate hive jars.
RUN set -eux; \
    curl -fSL -o ${JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fSL -o ${JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fSL -o ${JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar; \
    chown -R 1001:1001 ${JARS_DIR}

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY start-spark.sh /usr/local/bin/start-spark.sh
RUN chmod +x /usr/local/bin/start-spark.sh \
    && chown -R 1001:1001 /opt/bitnami/spark/conf

USER 1001

EXPOSE 7077 8081 8082

ENTRYPOINT ["/usr/local/bin/start-spark.sh"]
