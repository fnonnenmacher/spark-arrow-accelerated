FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y llvm-8 \
        clang-8 \
        autoconf \
        bash \
        cmake \
        g++ \
        gcc \
        make \
        curl \
        zip \
        git \
        openjdk-8-jdk \
        wget \
        libz-dev \
        maven \
        && rm -rf /var/lib/apt/lists/*

# Download apache arrow
RUN wget -q http://apache.40b.nl/arrow/arrow-0.17.0/apache-arrow-0.17.0.tar.gz &&\
        mkdir /arrow &&\
        tar -xzf apache-arrow-0.17.0.tar.gz --strip-components 1 -C /arrow &&\
        rm -f apache-arrow-0.17.0.tar.gz

# Build and install apache arrow with plasma server & plasma jni lib
RUN mkdir /arrow/cpp/release &&\
        cd /arrow/cpp/release &&\
        cmake -DARROW_PLASMA=ON -DARROW_PLASMA_JAVA_CLIENT=ON -DARROW_PARQUET=ON -DARROW_GANDIVA=ON -DARROW_GANDIVA_JAVA=ON .. &&\
        make &&\
        make install &&\
        cp /arrow/cpp/release/release/libplasma_java.so /usr/local/lib/

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN     cd /arrow/java &&\
        mvn clean install -P arrow-jni -am -DskipTests -Darrow.cpp.build.dir=../../cpp/release/release

# Download Fletcher
RUN wget -q https://github.com/abs-tudelft/fletcher/archive/develop.zip &&\
        unzip -q develop.zip  &&\
        mv /fletcher-develop /fletcher &&\
        rm -f develop.zip

# Install Fletcher Runtime
RUN mkdir /fletcher/runtime/cpp/build &&\
        cd /fletcher/runtime/cpp/build &&\
        cmake .. &&\
        make &&\
        make install

# Install Fletcher Echo Platform
RUN mkdir /fletcher/platforms/echo/runtime/build &&\
        cd /fletcher/platforms/echo/runtime/build &&\
        cmake .. &&\
        make &&\
        make install

ENV ARROW_ROOT /arrow

ENV LOCAL_LIBRARY_PATH /usr/local/lib

VOLUME /app
WORKDIR /app

CMD []