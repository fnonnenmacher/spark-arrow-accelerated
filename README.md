# Accelerating Spark SQL by executing transparently subquerries on Fletcher FPGAs

This repository contains my work/implementation which I do during my Masterthesis at the [Accelerated Big Data Systems of TUDelft](https://www.tudelft.nl/en/eemcs/the-faculty/departments/quantum-computer-engineering/accelerated-big-data-systems/) 


> :warning: This project is in a very early stage and currently the goal is to understand the involved technologies. I aim to build a minimal runnable solution at first. Therefor, the code is very volatile and sometimes aiming for a dirty solution.

## Goal

* Mapping of SparkSQL queries to FPGA building blocks and execute them by using [Fletcher](https://github.com/abs-tudelft/fletcher) & [Appache Arrow](https://github.com/apache/arrow)

## Architecture Overview Draft

![Architecture Draft](images/architecture-draft.png)

## Archived Steps

* Replace a pattern (Adding of three fields) in a projection with a projection which is then executed in native code (later Fletcher)
* Convert Spark partition to Apache Arrow Vector
* Store Apache Arrow Vector in Plasma Store
* Call native code from JNI
* Call most improtant Fletcher interfaces (atm without real kernel)
* Sum up Arrow vectors in C++
* Send result back to java (plasma-store) and convert to Spark Rows

## Build 

1. [Build Appache Arrow](https://arrow.apache.org/docs/developers/cpp/building.html) with the options `DARROW_PLASMA=ON` and `DARROW_PLASMA_JAVA_CLIENT=ON`
2. Install it from the release dir: `sudo make install`
3. Add the plasma-java library manually `release/libplasma_java.so /usr/local/lib64/` or `cp release/libplasma_java.dylib /usr/local/lib`
4. [Start the plasma server](https://github.com/apache/arrow/blob/master/cpp/apidoc/tutorials/plasma.md) e.g. `./plasma-store-server -m 1000000000 -s /tmp/plasma`
5. Run the gradle build (including tests) `./gradlew build`
6. Install Fletcher (TODO)

## Project structure

* `spark-extension`: this code integrates with spark and is replacing the default strategy of generating the execution plan. Instead, it takes the default Projection and splits it up, so that a part of it can be executed on Fletcher (for now C++)
* `arrow-processor` scala part which forward the Arrow vector to the native code bw writing it into the Plasma store
* `arrow-processor-native` code that is responsible for the execution of the processing steps. For now this is sums up two vectors, later it should call Fletcher.

## Ideas & Decisions

This [Google Slides] roughly shows different ideas,  