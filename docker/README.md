# Docker image for executing the file
 
* Uses [ubuntu:20.04](https://hub.docker.com/_/ubuntu) as base image
* Installs Apache Arrow, including parquet reading, dataset api and the gandiva library
* Installs fletcher runtime and fletcher platform echo
* published on Dockerhub as [fnonnenmacher/arrow-gandiva-java](https://hub.docker.com/r/fnonnenmacher/arrow-gandiva-fletcher)

## Version
* `arrow-plasma-java:0.1`: Contains jdk-8, apache arrow (including plasma)
* `arrow-plasma-java:0.2`: Adds fletcher runtime and echo platform 
* `arrow-plasma-java:0.3`: Adds arrow parquet library
* `arrow-plasma-java:0.4`: Adds llvm-8 and arrow `gandiva`, changes base image to ubuntu (on alpine llvm-8 could not be installed easily)
* `arrow-plasma-java:0.5`: Add parquet reading, remove plasma
* `arrow-plasma-java:0.6`: Add apache arrow dataset

* `arrow-gandiva-fletcher:0.7`: Rename docker file, update arrow to 0.17.1, Add snappy compression

## Commands:
* Build: `docker build -t fnonnenmacher/arrow-gandiva-fletcher:<VERSION> .`
* Push: `docker push fnonnenmacher/arrow-gandiva-fletcher:<VERSION>`