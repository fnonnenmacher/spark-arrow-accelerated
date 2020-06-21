# Accelerating Spark SQL by executing transparently subquerries on Fletcher FPGAs

This repository contains my work/implementation which I do during my Masterthesis at the [Accelerated Big Data Systems of TUDelft](https://www.tudelft.nl/en/eemcs/the-faculty/departments/quantum-computer-engineering/accelerated-big-data-systems/) 

> :warning: The goal of this project is to elaborate different technologies and how to integrate them with Spark. I want to understand the technical feasibility of different scenarios and want to figure out which problems appear and need to be solved. Therefore, this code is very volatile and does not always follow best practices.

## Goal

* Mapping of SparkSQL queries to FPGA building blocks and execute them by using [Fletcher](https://github.com/abs-tudelft/fletcher) & [Appache Arrow](https://github.com/apache/arrow)

## Architecture Overview Draft

![Architecture Draft](images/architecture-draft.png)

## Installation of required Software

Because of the many involved technologies the installation of the pre-conditions is not trivial. This [installation guide](Installation.md) describes the necessary things to do especially on machines without sudo rights (e.g. on the servers of the research group). Furthermore, if the execution on real FPGAs is not required the [docker image](/docker) used for the CI builds can be used: 

## Executing the project

The project is setup with [gradle](https://gradle.org/) which allowed me to create a multi-language build. For including locally installed libraries the following env vars can be used:

* `LD_LIBRARY_PATH`: colon separated list of directories to scan for local libraries. Evaluated before the default paths (`/usr/local/lib` and `usr/local/lib64`). E.g. `$WORK/local/lib:$WORK/local/lib64`
* `ADDITIONAL_INCLUDES`: colon separated list of directories of additional includes e.g. `$WORK/local/include/`
* `GANDIVA_JAR_DIR`: path to the directory containing the gandiva jar when not on MacOs. E.g. `$WORK/arrow-apache-arrow-0.17.1/java/gandiva/target/`
* `FLETCHER_PLATFORM`: fletcher platfrom which should be loaded e.g. `fletcher_snap`

> Hint: Stop the gradlew daemon './gradlew --stop' to make sure the new environment variables are passed to the gradle process.

Build the project including tests:
```
./gradlew build
```

To run individual tests: 
```
./gradlew :spark-extension:test --tests *.FletcherReductionExampleSuite
```

## Project structure

* `spark-extension`: this code integrates with spark and is replacing the default strategy of generating the execution plan.
* `arrow-processor` scala part which forward the Arrow vectors to the native code
* `arrow-processor-native` code that is responsible for the execution of the processing steps. For now it calls different C++ components e.g. the arrow parquet reader & gandiva) later it should call a Fletcher runtime. 