# Hive Metastore 4.0.0 plus the jars its classpath needs:
#   - Postgres JDBC, so schematool can talk to our `metastore` database
#   - hadoop-aws + aws-java-sdk-bundle, so HMS can validate S3A location
#     URIs when Spark/Trino create a namespace pointing at s3a://warehouse/
#
# The upstream image does not bundle any of these even when DB_DRIVER=postgres
# and the metastore is configured against an S3-backed warehouse.
FROM apache/hive:4.0.0

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

# Docker's ADD can fetch from a URL — avoids needing curl/wget in the base image.
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER root
RUN chown hive:hive /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar \
                    /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
                    /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
USER hive
