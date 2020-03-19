FROM openjdk:8-jdk-alpine

RUN apk add autoconf \
        bash \
        cmake \
        g++ \
        gcc \
        make \
        curl \
        zip

RUN wget -q https://github.com/apache/arrow/archive/master.zip &&\
        unzip -q master.zip  &&\
        mv /arrow-master /arrow &&\
        rm -f master.zip

RUN mkdir /arrow/cpp/release &&\
        cd /arrow/cpp/release &&\
        cmake -DARROW_PLASMA=ON -DARROW_PLASMA_JAVA_CLIENT=ON .. &&\
        make &&\
        make install &&\
        cp /arrow/cpp/release/release/libplasma_java.so /usr/local/lib64/ &&\
        cp /arrow/cpp/release/release/plasma-store-server /usr/local/lib64/

VOLUME /app
WORKDIR /app

CMD []

