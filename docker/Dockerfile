FROM apache/airflow:2.8.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget curl tar bash ca-certificates gnupg2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_VERSION=11
ENV JAVA_DISTRO=temurin
ENV JAVA_HOME=/opt/java/openjdk

RUN mkdir -p $JAVA_HOME \
    && curl -L https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.22+7/OpenJDK11U-jdk_x64_linux_hotspot_11.0.22_7.tar.gz \
    | tar -xz -C $JAVA_HOME --strip-components=1

ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV SPARK_VERSION=3.5.0
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar xz -C /opt/ \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm -f spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

ENV HADOOP_VERSION=3.3.4
RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt/

RUN mkdir -p /opt/spark/jars \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/common/*.jar /opt/spark/jars/ \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/common/lib/*.jar /opt/spark/jars/ \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/hdfs/*.jar /opt/spark/jars/ \
    && cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/*.jar /opt/spark/jars/

WORKDIR /opt/spark/jars
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar



USER airflow

RUN pip install apache-airflow-providers-apache-spark pyspark
