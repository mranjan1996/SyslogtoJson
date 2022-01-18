FROM centos
ENV DAEMON_RUN=true
ENV SPARK_VERSION=2.4.7
ENV HADOOP_VERSION=2.7
WORKDIR /opt/application
RUN yum -y install python36 wget java-1.8.0-openjdk
ENV PYSPARK_PYTHON python3.6
ENV PYSPARK_DRIVER_PYTHON python3.6
RUN ln -s /usr/bin/python3.6 /usr/local/bin/python
RUN wget https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py \
    && pip3.6 install numpy==1.19 pandas==1.1.5 pyspark==3.0.2
RUN wget --no-verbose http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ENV JAVA_HOME /usr/lib/jvm/jre
COPY syslogparser/main.py .
RUN chmod +x /opt/application/main.py
CMD ["/opt/application/main.py"]