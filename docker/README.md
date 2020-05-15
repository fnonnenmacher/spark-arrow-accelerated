# Docker image for executing the file
 
* Uses [ubuntu:20.04](https://hub.docker.com/_/ubuntu) as base image
* Installs Apache Arrow in the image with the JNI plasma and gandiva library
* Installs fletcher runtime and fletcher platform echo
* published on Dockerhub as [fnonnenmacher/arrow-plasma-java](https://hub.docker.com/r/fnonnenmacher/arrow-plasma-java)

## Version
* `0.1`: Contains jdk-8, apache arrow (including plasma)
* `0.2`: Adds fletcher runtime and echo platform 
* `0.3`: Adds arrow parquet library
* `0.4`: Adds llvm-8 and arrow `gandiva`, changes base image to ubuntu (on alpine llvm-8 could not be installed easily)
* `0.5`: Remove plasma, add  

## Commands:
* Build: `docker build -t fnonnenmacher/arrow-plasma-java:<VERSION> .`
* Push: `docker push fnonnenmacher/arrow-plasma-java:<VERSION>`