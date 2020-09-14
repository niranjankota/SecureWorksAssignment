FROM ubuntu:latest
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk
RUN apt-get update
RUN apt-get install git -y
RUN apt-get update
RUN apt-get install wget -y
RUN wget "https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz"
RUN tar -xzvf spark-3.0.1-bin-hadoop2.7.tgz
RUN rm spark-3.0.1-bin-hadoop2.7.tgz
RUN set -ex && \
    mkdir -p /root/jars && \
    mkdir -p /root/config && \
    mkdir -p /root/data
COPY param_secureworks_assignment.txt /root/config
COPY target/scala-2.12/secureworksassignment_2.12-0.1.jar /root/jars
CMD ./spark-3.0.1-bin-hadoop2.7/bin/spark-submit --num-executors 2 --executor-cores 2 --executor-memory 1g --driver-cores 1 --driver-memory 1g  \
/root/jars/secureworksassignment_2.12-0.1.jar /root/config/param_secureworks_assignment.txt



